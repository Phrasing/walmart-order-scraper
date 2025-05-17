package main

import (
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
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
)

type Order struct {
	MsgID       string
	Email       string
	OrderID     string
	ProductName string
	IsCanceled  bool
	Subject     string
}

var (
	reOrderConfirmation = regexp.MustCompile(`(?i)thanks for your order|order confirmation`)
	reOrderCancellation = regexp.MustCompile(`(?i)Canceled:.*?order #([\d-]+)|your order.*?has been canceled`)
	reOrderNumber       = regexp.MustCompile(`Order number:.*?(\d{7}-\d{8})`)
	reProductName       = regexp.MustCompile(`quantity \d+ item (.*)`)
)

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

	writeStats(orders)
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

	if reOrderConfirmation.MatchString(subject) {
		order.IsCanceled = false

		if body != "" {
			if matches := reOrderNumber.FindStringSubmatch(body); len(matches) > 1 {
				order.OrderID = strings.ReplaceAll(matches[1], "-", "")
			}

			order.ProductName = extractProductName(body)
		}

		if order.OrderID == "" {
			order.OrderID = "unknown_" + msgID
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

func writeStats(orders []Order) {
	if len(orders) == 0 {
		log.Println("No orders to process")
		return
	}

	confirmedByEmail := make(map[string]map[string]string)
	canceledByEmail := make(map[string]map[string]bool)

	for _, order := range orders {
		if order.IsCanceled {
			if _, exists := canceledByEmail[order.Email]; !exists {
				canceledByEmail[order.Email] = make(map[string]bool)
			}
			canceledByEmail[order.Email][order.OrderID] = true
		} else {
			if _, exists := confirmedByEmail[order.Email]; !exists {
				confirmedByEmail[order.Email] = make(map[string]string)
			}
			confirmedByEmail[order.Email][order.OrderID] = order.ProductName
		}
	}

	productStats := make(map[string]struct {
		Confirmed   int
		NonCanceled int
	})

	for email, orderMap := range confirmedByEmail {
		for orderID, product := range orderMap {
			canceled := false
			if cancelMap, exists := canceledByEmail[email]; exists {
				if _, wasCanceled := cancelMap[orderID]; wasCanceled {
					canceled = true
				}
			}

			stats := productStats[product]
			stats.Confirmed++
			if !canceled {
				stats.NonCanceled++
			}
			productStats[product] = stats
		}
	}

	timestamp := time.Now().Format("20060102_150405")
	filename := fmt.Sprintf("%s_%s.csv", csvFileBaseName, timestamp)

	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed to create CSV file: %v", err)
	}
	defer file.Close()

	writer := csv.NewWriter(file)
	defer writer.Flush()

	writeOverallStats(writer, confirmedByEmail, canceledByEmail)

	writeProductStats(writer, productStats)

	writeEmailStats(writer, confirmedByEmail, canceledByEmail)

	log.Printf("Statistics written to %s", filename)
}

func writeOverallStats(writer *csv.Writer, confirmed map[string]map[string]string, canceled map[string]map[string]bool) {
	writer.Write([]string{"Overall Stats"})
	writer.Write([]string{"Metric", "Count"})

	totalConfirmations := 0
	for _, orders := range confirmed {
		totalConfirmations += len(orders)
	}

	totalCancellations := 0
	for _, orders := range canceled {
		totalCancellations += len(orders)
	}

	nonCanceledCount := 0
	for email, confirmedOrders := range confirmed {
		for orderID := range confirmedOrders {
			if canceledOrders, exists := canceled[email]; !exists || !canceledOrders[orderID] {
				nonCanceledCount++
			}
		}
	}

	writer.Write([]string{"Total Confirmations", fmt.Sprintf("%d", totalConfirmations)})
	writer.Write([]string{"Total Cancellations", fmt.Sprintf("%d", totalCancellations)})
	writer.Write([]string{"Total Non-Cancelled Orders", fmt.Sprintf("%d", nonCanceledCount)})
	writer.Write([]string{""})
}

func writeProductStats(writer *csv.Writer, stats map[string]struct{ Confirmed, NonCanceled int }) {
	writer.Write([]string{"Per-Product Stats"})
	writer.Write([]string{"Product", "Total Confirmed", "Non-Cancelled", "Stick Rate (%)"})

	type productEntry struct {
		name        string
		confirmed   int
		nonCanceled int
	}

	var products []productEntry
	for name, stat := range stats {
		products = append(products, productEntry{
			name:        name,
			confirmed:   stat.Confirmed,
			nonCanceled: stat.NonCanceled,
		})
	}

	sort.Slice(products, func(i, j int) bool {
		return products[i].nonCanceled > products[j].nonCanceled
	})

	for _, p := range products {
		stickRate := 0.0
		if p.confirmed > 0 {
			stickRate = float64(p.nonCanceled) / float64(p.confirmed) * 100
		}

		writer.Write([]string{
			p.name,
			fmt.Sprintf("%d", p.confirmed),
			fmt.Sprintf("%d", p.nonCanceled),
			fmt.Sprintf("%.2f", stickRate),
		})
	}

	writer.Write([]string{""})
}

func writeEmailStats(writer *csv.Writer, confirmed map[string]map[string]string, canceled map[string]map[string]bool) {
	writer.Write([]string{"Per-Email Account Stats"})

	allEmails := make(map[string]bool)
	for email := range confirmed {
		allEmails[email] = true
	}
	for email := range canceled {
		allEmails[email] = true
	}

	type emailStat struct {
		email              string
		nonCanceled        int
		totalCancellations int
	}

	var stats []emailStat
	for email := range allEmails {
		stat := emailStat{email: email}

		if confirmedOrders, exists := confirmed[email]; exists {
			for orderID := range confirmedOrders {
				isCanceled := false
				if canceledOrders, exists := canceled[email]; exists && canceledOrders[orderID] {
					isCanceled = true
				}

				if !isCanceled {
					stat.nonCanceled++
				}
			}
		}

		if canceledOrders, exists := canceled[email]; exists {
			stat.totalCancellations = len(canceledOrders)
		}

		stats = append(stats, stat)
	}

	writer.Write([]string{"Accounts with Most Non-Cancelled Orders (Sticks)"})
	writer.Write([]string{"Email Address", "Non-Cancelled Orders", "Total Cancellations"})

	sort.Slice(stats, func(i, j int) bool {
		return stats[i].nonCanceled > stats[j].nonCanceled
	})

	for _, stat := range stats {
		if stat.nonCanceled > 0 {
			writer.Write([]string{
				stat.email,
				fmt.Sprintf("%d", stat.nonCanceled),
				fmt.Sprintf("%d", stat.totalCancellations),
			})
		}
	}
	writer.Write([]string{""})

	writer.Write([]string{"Accounts with Cancellations and No Non-Cancelled Orders"})
	writer.Write([]string{"Email Address", "Total Cancellations", "Non-Cancelled Orders"})

	for _, stat := range stats {
		if stat.totalCancellations > 0 && stat.nonCanceled == 0 {
			writer.Write([]string{
				stat.email,
				fmt.Sprintf("%d", stat.totalCancellations),
				"0",
			})
		}
	}
	writer.Write([]string{""})
}
