package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"
)

const (
	tokenFile        = "token.json"
	credentialsFile  = "credentials.json"
	emailQuery       = "from:help@walmart.com newer_than:30d"
	numWorkers       = 3
	maxRetryAttempts = 3
	baseRetrySleep   = time.Second
	apiRateDelayMs   = 50
	csvFileBaseName  = "walmart_order_stats"
	htmlFileBaseName = "walmart_order_report"
)

// ReportData holds all data for the HTML template
type ReportData struct {
	Timestamp              string
	Overall                OverallStatsData
	ProductStats           []ProductStatData
	ShippedOrders          []ShippedOrderData
	PendingShipmentOrders  []PendingShipmentData  // This is actually "Confirmed but not Shipped and not Cancelled"
	AwaitingShipmentOrders []AwaitingShipmentData // This will be "Not Cancelled and No Shipping Email"
	EmailStats             EmailStatsDataContainer
}

type OverallStatsData struct {
	TotalConfirmations int
	TotalCancellations int
	TotalNonCancelled  int
	TotalShipped       int
}

type ProductStatData struct {
	Name        string
	Confirmed   int
	NonCanceled int
	Shipped     int
	StickRate   float64
}

type ShippedOrderData struct {
	Email          string
	OrderID        string
	ProductName    string
	TrackingNumber string
	TrackingLink   string // Added for carrier tracking link
}

type PendingShipmentData struct {
	Email       string
	OrderID     string
	ProductName string
}

// AwaitingShipmentData represents orders that are confirmed, not canceled, but for which we haven't received a shipping email.
type AwaitingShipmentData struct {
	Email       string
	OrderID     string
	ProductName string
}

type EmailStatData struct {
	Email              string
	NonCanceled        int
	TotalCancellations int
}

type EmailStatsDataContainer struct {
	TopNonCancelled   []EmailStatData
	OnlyCancellations []EmailStatData
}

type Order struct {
	MsgID          string
	Email          string
	OrderID        string
	ProductName    string
	IsCanceled     bool
	IsShipped      bool
	TrackingNumber string
	Subject        string
}

var (
	reOrderConfirmation = regexp.MustCompile(`(?i)thanks for your order|order confirmation`)
	reOrderCancellation = regexp.MustCompile(`(?i)Canceled:.*?order #([\d-]+)|your order.*?has been canceled`)
	reOrderShipped      = regexp.MustCompile(`(?i)Shipped:`)
	reOrderNumber       = regexp.MustCompile(`Order number:.*?(\d{7}-\d{8})`)
	reProductName       = regexp.MustCompile(`quantity \d+ item ([^"<]+)`)
	reTrackingNumber    = regexp.MustCompile(`(?i)tracking number <a[^>]*>([^<]+)</a>`)
)

// getTrackingLink attempts to determine the carrier and return a direct tracking link.
func getTrackingLink(trackingNumber string) string {
	trackingNumber = strings.TrimSpace(trackingNumber)
	if trackingNumber == "" || trackingNumber == "N/A" {
		return ""
	}

	// USPS
	// Common patterns: 20-22 digits, or 13 char international (e.g., XX000000000XX)
	// Specific prefixes like 9400, 9205, 9303 for domestic.
	if (len(trackingNumber) >= 20 && len(trackingNumber) <= 22 && regexp.MustCompile(`^\d+$`).MatchString(trackingNumber)) ||
		(len(trackingNumber) == 13 && regexp.MustCompile(`^[A-Z]{2}\d{9}[A-Z]{2}$`).MatchString(trackingNumber)) ||
		(regexp.MustCompile(`^9[2-4]\d{18,20}$`).MatchString(trackingNumber)) { // More specific USPS domestic
		return fmt.Sprintf("https://tools.usps.com/go/TrackConfirmAction?tLabels=%s", trackingNumber)
	}

	// UPS: Starts with 1Z, 18 chars total.
	if strings.HasPrefix(trackingNumber, "1Z") && len(trackingNumber) == 18 && regexp.MustCompile(`^1Z[0-9A-Z]{16}$`).MatchString(trackingNumber) {
		return fmt.Sprintf("https://www.ups.com/track?loc=en_US&tracknum=%s", trackingNumber)
	}

	// FedEx: Common lengths 12 (Express), 15 (Ground), also 20, 22. Typically all digits.
	// Prefixes like 96..., 6..., 7..., 56...
	if (len(trackingNumber) == 12 || len(trackingNumber) == 15 || len(trackingNumber) == 20 || len(trackingNumber) == 22) && regexp.MustCompile(`^\d+$`).MatchString(trackingNumber) {
		if strings.HasPrefix(trackingNumber, "96") || // FedEx Express (new)
			strings.HasPrefix(trackingNumber, "6") || // FedEx Ground (often 15 digits starting with 6)
			strings.HasPrefix(trackingNumber, "7") || // FedEx Express (common 12 digits)
			strings.HasPrefix(trackingNumber, "56") || // FedEx SmartPost
			len(trackingNumber) == 12 || len(trackingNumber) == 15 { // General catch for typical lengths
			return fmt.Sprintf("https://www.fedex.com/fedextrack/?trknbr=%s", trackingNumber)
		}
	}
	// Amazon Logistics (TBA numbers)
	if strings.HasPrefix(trackingNumber, "TBA") && regexp.MustCompile(`^TBA[A-Z0-9]{12,15}$`).MatchString(trackingNumber) { // Adjusted regex for TBA
		return fmt.Sprintf("https://www.amazon.com/progress-tracker/package/%s", trackingNumber)
	}

	// If no specific carrier matched, return empty or a generic search link
	// return fmt.Sprintf("https://www.google.com/search?q=track+%s", trackingNumber) // Optional: generic search
	return ""
}

func main() {
	ctx := context.Background()
	srv := initGmailService(ctx)

	log.Printf("Searching for emails with query: %s\n", emailQuery)
	messageIDs := searchEmails(srv)
	log.Printf("Found %d emails matching the query\n", len(messageIDs))

	if len(messageIDs) == 0 {
		fmt.Println("No emails found matching the criteria.")
		return
	}

	orders := processEmails(srv, messageIDs)
	log.Printf("Processed %d orders from emails\n", len(orders))

	reportData := generateReportData(orders)
	writeCSVStats(reportData) // Renamed from writeStats
	writeHTMLReport(reportData)
}

func initGmailService(ctx context.Context) *gmail.Service {
	tokenPath := os.Getenv("GMAIL_TOKEN_FILE")
	if tokenPath == "" {
		tokenPath = tokenFile
	}

	credsPath := os.Getenv("GMAIL_CREDS_FILE")
	if credsPath == "" {
		credsPath = credentialsFile
	}

	credBytes, err := os.ReadFile(credsPath)
	if err != nil {
		log.Fatalf("Unable to read credentials file: %v", err)
	}

	config, err := google.ConfigFromJSON(credBytes, gmail.GmailReadonlyScope)
	if err != nil {
		log.Fatalf("Unable to parse credentials: %v", err)
	}

	token, err := getToken(config, tokenPath)
	if err != nil {
		log.Fatalf("Unable to get authentication token: %v", err)
	}

	srv, err := gmail.NewService(ctx, option.WithHTTPClient(config.Client(ctx, token)))
	if err != nil {
		log.Fatalf("Unable to create Gmail service: %v", err)
	}

	return srv
}

func getToken(config *oauth2.Config, tokenPath string) (*oauth2.Token, error) {
	token, err := readTokenFromFile(tokenPath)
	if err == nil {
		return token, nil
	}

	log.Printf("Token file not found or invalid. Getting new token from web.")
	token = getTokenFromWeb(config)

	saveToken(tokenPath, token)
	return token, nil
}

func readTokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	defer f.Close()

	token := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(token)
	return token, err
}

func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to this URL in your browser, then enter the code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatalf("Unable to read auth code: %v", err)
	}

	token, err := config.Exchange(context.TODO(), code)
	if err != nil {
		log.Fatalf("Unable to get token from web: %v", err)
	}
	return token
}

func saveToken(path string, token *oauth2.Token) {
	f, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0600)
	if err != nil {
		log.Printf("Unable to save token: %v", err)
		return
	}
	defer f.Close()

	json.NewEncoder(f).Encode(token)
	log.Printf("Token saved to: %s", path)
}

func searchEmails(srv *gmail.Service) []string {
	var messageIDs []string
	pageToken := ""

	for {
		req := srv.Users.Messages.List("me").Q(emailQuery)
		if pageToken != "" {
			req.PageToken(pageToken)
		}

		res, err := req.Do()
		if err != nil {
			log.Fatalf("Failed to search emails: %v", err)
		}

		for _, msg := range res.Messages {
			messageIDs = append(messageIDs, msg.Id)
		}

		if res.NextPageToken == "" {
			break
		}

		pageToken = res.NextPageToken
		time.Sleep(apiRateDelayMs * time.Millisecond)
	}

	return messageIDs
}

func processEmails(srv *gmail.Service, messageIDs []string) []Order {
	var orders []Order
	var mu sync.Mutex
	var wg sync.WaitGroup

	idChan := make(chan string, len(messageIDs))
	for _, id := range messageIDs {
		idChan <- id
	}
	close(idChan)

	for i := 0; i < numWorkers; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()

			for id := range idChan {
				order, ok := processSingleEmail(srv, id, workerID)
				if ok {
					mu.Lock()
					orders = append(orders, order)
					mu.Unlock()
				}
			}
		}(i)
	}

	wg.Wait()
	return orders
}

func processSingleEmail(srv *gmail.Service, msgID string, workerID int) (Order, bool) {
	var msg *gmail.Message
	var err error

	for attempt := 0; attempt < maxRetryAttempts; attempt++ {
		msg, err = srv.Users.Messages.Get("me", msgID).Format("full").Do()
		if err == nil {
			break
		}

		log.Printf("[Worker %d] Error getting message %s (attempt %d): %v",
			workerID, msgID, attempt+1, err)

		if strings.Contains(err.Error(), "rateLimitExceeded") ||
			strings.Contains(err.Error(), "userRateLimitExceeded") ||
			strings.Contains(err.Error(), "Quota exceeded") {
			sleepTime := baseRetrySleep * time.Duration(attempt+1)
			time.Sleep(sleepTime)
		} else {
			return Order{}, false
		}
	}

	if err != nil {
		log.Printf("[Worker %d] Failed to get message %s after %d attempts",
			workerID, msgID, maxRetryAttempts)
		return Order{}, false
	}

	var subject, recipient string
	for _, header := range msg.Payload.Headers {
		if header.Name == "Subject" {
			subject = header.Value
		} else if header.Name == "To" {
			recipient = header.Value
		}
	}

	recipient = extractEmailAddress(recipient)
	if recipient == "" {
		log.Printf("[Worker %d] Could not parse recipient from message %s", workerID, msgID)
		return Order{}, false
	}

	body := extractBody(msg.Payload)

	order := Order{
		MsgID:   msgID,
		Email:   recipient,
		Subject: subject,
	}

	if matches := reOrderCancellation.FindStringSubmatch(subject); len(matches) > 1 {
		order.IsCanceled = true
		order.OrderID = strings.ReplaceAll(matches[1], "-", "")
		log.Printf("[Worker %d] Found cancellation: Order %s for %s",
			workerID, order.OrderID, order.Email)
		return order, true
	}

	if reOrderShipped.MatchString(subject) {
		order.IsShipped = true
		if body != "" {
			if matches := reOrderNumber.FindStringSubmatch(body); len(matches) > 1 {
				order.OrderID = strings.ReplaceAll(matches[1], "-", "")
			}
			order.ProductName = extractProductName(body)
			if matches := reTrackingNumber.FindStringSubmatch(body); len(matches) > 1 {
				order.TrackingNumber = strings.TrimSpace(matches[1])
			}
		}
		if order.OrderID == "" { // Fallback if not found in body, check subject (though less likely for shipped)
			subjectOrderMatches := reOrderNumber.FindStringSubmatch(subject)
			if len(subjectOrderMatches) > 1 {
				order.OrderID = strings.ReplaceAll(subjectOrderMatches[1], "-", "")
			} else {
				order.OrderID = "unknown_shipped_" + msgID
			}
		}
		if order.ProductName == "" {
			order.ProductName = "Unknown Product (Shipped)"
		}
		if order.TrackingNumber == "" {
			order.TrackingNumber = "N/A"
		}
		log.Printf("[Worker %d] Found shipment: Order %s, Product '%s', Tracking '%s' for %s",
			workerID, order.OrderID, order.ProductName, order.TrackingNumber, order.Email)
		return order, true
	}

	if reOrderConfirmation.MatchString(subject) {
		order.IsCanceled = false
		order.IsShipped = false
		if body != "" {
			if matches := reOrderNumber.FindStringSubmatch(body); len(matches) > 1 {
				order.OrderID = strings.ReplaceAll(matches[1], "-", "")
			}
			order.ProductName = extractProductName(body)
		}

		if order.OrderID == "" {
			order.OrderID = "unknown_confirm_" + msgID
		}
		if order.ProductName == "" {
			order.ProductName = "Unknown Product"
		}
		log.Printf("[Worker %d] Found confirmation: Order %s, Product '%s' for %s",
			workerID, order.OrderID, order.ProductName, order.Email)
		return order, true
	}

	return Order{}, false
}

func extractEmailAddress(header string) string {
	if header == "" {
		return ""
	}

	if start := strings.LastIndex(header, "<"); start != -1 {
		if end := strings.LastIndex(header, ">"); end > start {
			return strings.ToLower(header[start+1 : end])
		}
	}

	return strings.ToLower(strings.TrimSpace(header))
}

func extractBody(payload *gmail.MessagePart) string {
	if payload.Body != nil && payload.Body.Data != "" {
		if payload.MimeType == "text/plain" || payload.MimeType == "text/html" {
			data, err := base64.URLEncoding.DecodeString(payload.Body.Data)
			if err == nil {
				return string(data)
			}
		}
	}

	if payload.Parts != nil {
		for _, part := range payload.Parts {
			if part.MimeType == "text/html" && part.Body != nil && part.Body.Data != "" {
				data, err := base64.URLEncoding.DecodeString(part.Body.Data)
				if err == nil {
					return string(data)
				}
			}
		}

		for _, part := range payload.Parts {
			if part.MimeType == "text/plain" && part.Body != nil && part.Body.Data != "" {
				data, err := base64.URLEncoding.DecodeString(part.Body.Data)
				if err == nil {
					return string(data)
				}
			}
		}

		for _, part := range payload.Parts {
			if strings.HasPrefix(part.MimeType, "multipart/") && part.Parts != nil {
				if body := extractBody(part); body != "" {
					return body
				}
			}
		}
	}

	return ""
}

func extractProductName(body string) string {
	matches := reProductName.FindStringSubmatch(body)
	if len(matches) > 1 {
		return strings.TrimSpace(matches[1])
	}
	return "Unknown Product"
}

func generateReportData(orders []Order) ReportData {
	reportTimestamp := time.Now().Format(time.RFC1123)
	if len(orders) == 0 {
		log.Println("No orders to process for report data generation")
		return ReportData{Timestamp: reportTimestamp}
	}

	confirmedByEmail := make(map[string]map[string]string)
	canceledByEmail := make(map[string]map[string]bool)
	shippedOrdersInfo := make(map[string]map[string]Order)

	for _, order := range orders {
		if order.IsCanceled {
			if _, exists := canceledByEmail[order.Email]; !exists {
				canceledByEmail[order.Email] = make(map[string]bool)
			}
			canceledByEmail[order.Email][order.OrderID] = true
		} else if order.IsShipped {
			if _, exists := shippedOrdersInfo[order.Email]; !exists {
				shippedOrdersInfo[order.Email] = make(map[string]Order)
			}
			shippedOrdersInfo[order.Email][order.OrderID] = order
			if _, exists := confirmedByEmail[order.Email]; !exists {
				confirmedByEmail[order.Email] = make(map[string]string)
			}
			if _, productExists := confirmedByEmail[order.Email][order.OrderID]; !productExists {
				confirmedByEmail[order.Email][order.OrderID] = order.ProductName
			}
		} else {
			if _, exists := confirmedByEmail[order.Email]; !exists {
				confirmedByEmail[order.Email] = make(map[string]string)
			}
			confirmedByEmail[order.Email][order.OrderID] = order.ProductName
		}
	}

	var overallData OverallStatsData
	for _, orderMap := range confirmedByEmail {
		overallData.TotalConfirmations += len(orderMap)
	}
	for _, cancelMap := range canceledByEmail {
		overallData.TotalCancellations += len(cancelMap)
	}
	for email, orderMap := range confirmedByEmail {
		for orderID := range orderMap {
			if cancelMap, exists := canceledByEmail[email]; !exists || !cancelMap[orderID] {
				overallData.TotalNonCancelled++
			}
		}
	}
	for _, shippedMap := range shippedOrdersInfo {
		overallData.TotalShipped += len(shippedMap)
	}

	productStatsMap := make(map[string]struct {
		Confirmed   int
		NonCanceled int
		Shipped     int
	})
	for email, orderMap := range confirmedByEmail {
		for orderID, product := range orderMap {
			isCanceled := false
			if cancelMap, exists := canceledByEmail[email]; exists && cancelMap[orderID] {
				isCanceled = true
			}
			currentStat := productStatsMap[product]
			currentStat.Confirmed++
			if !isCanceled {
				currentStat.NonCanceled++
			}
			productStatsMap[product] = currentStat
		}
	}
	for _, shippedMap := range shippedOrdersInfo {
		for orderID, shippedOrder := range shippedMap {
			productNameForStat := shippedOrder.ProductName
			if productNameForStat == "" || strings.HasPrefix(productNameForStat, "Unknown Product") {
				if confirmedEmail, ok := confirmedByEmail[shippedOrder.Email]; ok {
					if pName, pOk := confirmedEmail[orderID]; pOk && pName != "" && !strings.HasPrefix(pName, "Unknown Product") {
						productNameForStat = pName
					}
				}
			}
			if productNameForStat == "" {
				productNameForStat = "Unknown Product (Shipped)"
			}
			currentStat := productStatsMap[productNameForStat]
			currentStat.Shipped++
			productStatsMap[productNameForStat] = currentStat
		}
	}
	var productStatsData []ProductStatData
	for name, stat := range productStatsMap {
		stickRate := 0.0
		if stat.Confirmed > 0 {
			stickRate = float64(stat.NonCanceled) / float64(stat.Confirmed) * 100
		}
		productStatsData = append(productStatsData, ProductStatData{
			Name:        name,
			Confirmed:   stat.Confirmed,
			NonCanceled: stat.NonCanceled,
			Shipped:     stat.Shipped,
			StickRate:   stickRate,
		})
	}
	sort.Slice(productStatsData, func(i, j int) bool {
		return productStatsData[i].NonCanceled > productStatsData[j].NonCanceled
	})

	var shippedOrdersData []ShippedOrderData
	// Concurrently process tracking links
	type trackingLinkJob struct {
		Email          string
		OrderID        string
		ProductName    string
		TrackingNumber string
	}

	numShippedOrders := 0
	for _, orderMap := range shippedOrdersInfo {
		numShippedOrders += len(orderMap)
	}

	jobs := make(chan trackingLinkJob, numShippedOrders)
	results := make(chan ShippedOrderData, numShippedOrders)
	var wgLinks sync.WaitGroup

	// Start workers for getTrackingLink
	// Using numWorkers for this as well, can be tuned.
	for w := 0; w < numWorkers; w++ {
		wgLinks.Add(1)
		go func() {
			defer wgLinks.Done()
			for job := range jobs {
				link := getTrackingLink(job.TrackingNumber)
				results <- ShippedOrderData{
					Email:          job.Email,
					OrderID:        job.OrderID,
					ProductName:    job.ProductName,
					TrackingNumber: job.TrackingNumber,
					TrackingLink:   link,
				}
			}
		}()
	}

	// Send jobs
	for email, orderMap := range shippedOrdersInfo {
		for orderID, orderDetails := range orderMap {
			jobs <- trackingLinkJob{
				Email:          email,
				OrderID:        orderID,
				ProductName:    orderDetails.ProductName,
				TrackingNumber: orderDetails.TrackingNumber,
			}
		}
	}
	close(jobs)

	// Collect results
	// wgLinks.Wait() should be called after closing results channel
	// but we need to collect results first.
	// A separate goroutine to close results channel once all workers are done.
	go func() {
		wgLinks.Wait()
		close(results)
	}()

	for sod := range results {
		shippedOrdersData = append(shippedOrdersData, sod)
	}

	sort.Slice(shippedOrdersData, func(i, j int) bool {
		if shippedOrdersData[i].Email != shippedOrdersData[j].Email {
			return shippedOrdersData[i].Email < shippedOrdersData[j].Email
		}
		return shippedOrdersData[i].OrderID < shippedOrdersData[j].OrderID
	})

	var pendingShipmentData []PendingShipmentData
	for email, orderMap := range confirmedByEmail {
		for orderID, productName := range orderMap {
			isCanceled := false
			if cancelMap, exists := canceledByEmail[email]; exists && cancelMap[orderID] {
				isCanceled = true
			}
			isShipped := false
			if shippedMap, exists := shippedOrdersInfo[email]; exists && shippedMap[orderID].MsgID != "" {
				isShipped = true
			}
			if !isCanceled && !isShipped {
				pendingShipmentData = append(pendingShipmentData, PendingShipmentData{
					Email:       email,
					OrderID:     orderID,
					ProductName: productName,
				})
			}
		}
	}
	sort.Slice(pendingShipmentData, func(i, j int) bool {
		if pendingShipmentData[i].Email != pendingShipmentData[j].Email {
			return pendingShipmentData[i].Email < pendingShipmentData[j].Email
		}
		return pendingShipmentData[i].OrderID < pendingShipmentData[j].OrderID
	})

	// Logic for AwaitingShipmentOrders (Not Cancelled, No Shipping Email)
	// This is subtly different from PendingShipmentOrders.
	// PendingShipmentOrders = Confirmed AND NOT Cancelled AND NOT Shipped (based on *any* shipping email for that order ID)
	// AwaitingShipmentOrders = Confirmed AND NOT Cancelled AND (No Shipping Email *at all* for that order ID from *any* source)
	// For this task, "we still don't have a shipping email for" implies checking against shippedOrdersInfo.
	// The existing PendingShipmentOrders logic already covers this.
	// Let's rename PendingShipmentOrders to AwaitingShipmentNoEmail to be more precise
	// and then ensure the logic correctly populates it.
	// The request is "all the orders that were not cancelled but we still don't have a shipping email for."
	// This is exactly what `pendingShipmentData` currently calculates.
	// So, we will use `pendingShipmentData` for the new `AwaitingShipmentOrders` field
	// and remove the `PendingShipmentOrders` field from ReportData as it's redundant with the new request.

	// Re-evaluating: The user asked for "another section".
	// `PendingShipmentOrders` is defined as: confirmed, not cancelled, not shipped.
	// The new request: "not cancelled but we still don't have a shipping email for".
	// These two are identical if "shipped" means "has a shipping email".
	// Let's assume the existing `PendingShipmentOrders` is what they want in this new section.
	// To avoid confusion and make it explicit, I will populate `AwaitingShipmentOrders` with the same data
	// as `PendingShipmentOrders`. The user can then decide if they want to remove the old "Pending Shipment" section
	// from the template later if it's truly redundant.

	var awaitingShipmentData []AwaitingShipmentData
	for email, orderMap := range confirmedByEmail {
		for orderID, productName := range orderMap {
			isCanceled := false
			if cancelMap, exists := canceledByEmail[email]; exists && cancelMap[orderID] {
				isCanceled = true
			}

			// Check if a shipping email exists for this specific orderID and email
			// AND if that shipping email is effectively for the 'productName' from the confirmation.
			productNameFromConfirmation := productName // productName is from confirmedByEmail[email][orderID]

			shippedAsThisProduct := false
			if shippingDetails, shippingRecordExists := shippedOrdersInfo[email][orderID]; shippingRecordExists {
				// A shipping record exists for this (email, orderID).
				// Now, determine the "effective" product name this shipping record contributes to in ProductStats.
				effectiveShippedProductName := shippingDetails.ProductName // Start with product name from the shipping email.

				if effectiveShippedProductName == "" || strings.HasPrefix(effectiveShippedProductName, "Unknown Product") {
					// If the shipping email's product name is generic,
					// check if the original confirmation (productNameFromConfirmation) had a more specific name.
					if productNameFromConfirmation != "" && !strings.HasPrefix(productNameFromConfirmation, "Unknown Product") {
						effectiveShippedProductName = productNameFromConfirmation
					}
					// If both confirmation and shipping names were generic, effectiveShippedProductName remains the generic one from the shipping email.
				}
				// Now, effectiveShippedProductName is the name that this shipped item would be tallied under in ProductStats.

				if effectiveShippedProductName == productNameFromConfirmation {
					// The item was shipped and its shipping record (after reconciliation) matches the confirmed product.
					shippedAsThisProduct = true
				}
			}
			// If no shippingRecordExists for this (email, orderID), shippedAsThisProduct remains false.

			if !isCanceled && !shippedAsThisProduct {
				awaitingShipmentData = append(awaitingShipmentData, AwaitingShipmentData{
					Email:       email,
					OrderID:     orderID,
					ProductName: productName,
				})
			}
		}
	}
	sort.Slice(awaitingShipmentData, func(i, j int) bool {
		if awaitingShipmentData[i].Email != awaitingShipmentData[j].Email {
			return awaitingShipmentData[i].Email < awaitingShipmentData[j].Email
		}
		return awaitingShipmentData[i].OrderID < awaitingShipmentData[j].OrderID
	})

	allEmails := make(map[string]bool)
	for email := range confirmedByEmail {
		allEmails[email] = true
	}
	for email := range canceledByEmail {
		allEmails[email] = true
	}
	var emailStatsList []EmailStatData
	for email := range allEmails {
		stat := EmailStatData{Email: email}
		if confirmedOrders, exists := confirmedByEmail[email]; exists {
			for orderID := range confirmedOrders {
				if cancelMap, cExists := canceledByEmail[email]; !cExists || !cancelMap[orderID] {
					stat.NonCanceled++
				}
			}
		}
		if cancelMap, exists := canceledByEmail[email]; exists {
			stat.TotalCancellations = len(cancelMap)
		}
		emailStatsList = append(emailStatsList, stat)
	}

	var topNonCancelled []EmailStatData
	var onlyCancellations []EmailStatData

	sort.Slice(emailStatsList, func(i, j int) bool {
		return emailStatsList[i].NonCanceled > emailStatsList[j].NonCanceled
	})
	for _, stat := range emailStatsList {
		if stat.NonCanceled > 0 {
			topNonCancelled = append(topNonCancelled, stat)
		}
		if stat.TotalCancellations > 0 && stat.NonCanceled == 0 {
			onlyCancellations = append(onlyCancellations, stat)
		}
	}

	return ReportData{
		Timestamp:              reportTimestamp,
		Overall:                overallData,
		ProductStats:           productStatsData,
		ShippedOrders:          shippedOrdersData,
		PendingShipmentOrders:  pendingShipmentData,  // Keep this for now, user might still want it
		AwaitingShipmentOrders: awaitingShipmentData, // Populate the new field
		EmailStats: EmailStatsDataContainer{
			TopNonCancelled:   topNonCancelled,
			OnlyCancellations: onlyCancellations,
		},
	}
}

func writeCSVStats(data ReportData) {
	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.csv", csvFileBaseName, timestamp)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()
	writer := csv.NewWriter(file)
	defer writer.Flush()

	writeOverallStatsCSV(writer, data.Overall)
	writeProductStatsCSV(writer, data.ProductStats)
	writeShippedOrdersListCSV(writer, data.ShippedOrders)
	writePendingShipmentOrdersCSV(writer, data.PendingShipmentOrders)   // Keep existing
	writeAwaitingShipmentOrdersCSV(writer, data.AwaitingShipmentOrders) // Add new
	writeEmailStatsCSV(writer, data.EmailStats)

	log.Printf("CSV statistics written to %s", filename)
}

func writeOverallStatsCSV(writer *csv.Writer, overall OverallStatsData) {
	writer.Write([]string{"Overall Stats"})
	writer.Write([]string{"Metric", "Count"})
	writer.Write([]string{"Total Confirmations", fmt.Sprintf("%d", overall.TotalConfirmations)})
	writer.Write([]string{"Total Cancellations", fmt.Sprintf("%d", overall.TotalCancellations)})
	writer.Write([]string{"Total Non-Cancelled Orders", fmt.Sprintf("%d", overall.TotalNonCancelled)})
	writer.Write([]string{"Total Shipped Orders", fmt.Sprintf("%d", overall.TotalShipped)})
}

func writeProductStatsCSV(writer *csv.Writer, productStats []ProductStatData) {
	writer.Write([]string{""})
	writer.Write([]string{"Per-Product Stats"})
	writer.Write([]string{"Product", "Total Confirmed", "Non-Cancelled", "Shipped", "Stick Rate (%)"})
	for _, p := range productStats {
		writer.Write([]string{
			p.Name,
			fmt.Sprintf("%d", p.Confirmed),
			fmt.Sprintf("%d", p.NonCanceled),
			fmt.Sprintf("%d", p.Shipped),
			fmt.Sprintf("%.2f", p.StickRate),
		})
	}
	writer.Write([]string{""})
}

func writeShippedOrdersListCSV(writer *csv.Writer, shippedOrders []ShippedOrderData) {
	writer.Write([]string{""})
	writer.Write([]string{"Shipped Orders Details"})
	writer.Write([]string{"Email", "Order ID", "Product Name", "Tracking Number", "Tracking Link"})
	for _, entry := range shippedOrders {
		writer.Write([]string{entry.Email, entry.OrderID, entry.ProductName, entry.TrackingNumber, entry.TrackingLink})
	}
	writer.Write([]string{""})
}

func writePendingShipmentOrdersCSV(writer *csv.Writer, pendingOrders []PendingShipmentData) {
	writer.Write([]string{""})
	writer.Write([]string{"Confirmed Orders - Pending Shipment (Not Canceled)"})
	writer.Write([]string{"Email", "Order ID", "Product Name"})
	for _, entry := range pendingOrders {
		writer.Write([]string{entry.Email, entry.OrderID, entry.ProductName})
	}
	writer.Write([]string{""})
}

func writeAwaitingShipmentOrdersCSV(writer *csv.Writer, awaitingOrders []AwaitingShipmentData) {
	writer.Write([]string{""})
	writer.Write([]string{"Orders Not Cancelled & No Shipping Email Yet"})
	writer.Write([]string{"Email", "Order ID", "Product Name"})
	for _, entry := range awaitingOrders {
		writer.Write([]string{entry.Email, entry.OrderID, entry.ProductName})
	}
	writer.Write([]string{""})
}

func writeEmailStatsCSV(writer *csv.Writer, emailStats EmailStatsDataContainer) {
	writer.Write([]string{""})
	writer.Write([]string{"Per-Email Account Stats"})

	writer.Write([]string{""})
	writer.Write([]string{"Top Accounts by Non-Cancelled Orders"})
	writer.Write([]string{"Email Address", "Non-Cancelled Orders", "Total Cancellations"})
	for _, stat := range emailStats.TopNonCancelled {
		writer.Write([]string{
			stat.Email,
			fmt.Sprintf("%d", stat.NonCanceled),
			fmt.Sprintf("%d", stat.TotalCancellations),
		})
	}
	writer.Write([]string{""})

	writer.Write([]string{""})
	writer.Write([]string{"Accounts with Only Cancellations (No Non-Cancelled Orders)"})
	writer.Write([]string{"Email Address", "Total Cancellations", "Non-Cancelled Orders"})
	for _, stat := range emailStats.OnlyCancellations {
		writer.Write([]string{
			stat.Email,
			fmt.Sprintf("%d", stat.TotalCancellations),
			"0",
		})
	}
	writer.Write([]string{""})
}

func writeHTMLReport(data ReportData) {
	tmpl, err := template.ParseFiles("report_template.html")
	if err != nil {
		log.Fatalf("Failed to parse HTML template: %v", err)
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.html", htmlFileBaseName, timestamp)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create HTML report file: %v", err)
	}
	defer file.Close()

	err = tmpl.Execute(file, data)
	if err != nil {
		log.Fatalf("Failed to execute HTML template: %v", err)
	}
	log.Printf("HTML report written to %s", filename)
}
