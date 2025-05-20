package main

import (
	"bytes" // Added for HTTP request body
	"context"
	"encoding/base64"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"html/template"
	"io" // Added for reading HTTP response
	"log"
	"net/http" // Added for direct API calls
	"net/url"  // Added for OAuth token request
	"os"

	// "os/exec" // No longer needed for MCP tool
	"regexp"
	"sort"
	"strings"
	"sync"
	"time"

	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"
	"google.golang.org/api/option"

	"github.com/joho/godotenv" // Added for .env file loading
)

const (
	tokenFile             = "token.json"
	credentialsFile       = "credentials.json"
	emailQuery            = "from:help@walmart.com newer_than:30d"
	numWorkers            = 2 // Reduced from 8 to 2 to lower Gmail API call rate
	maxRetryAttempts      = 3
	baseRetrySleep        = time.Second
	apiRateDelayMs        = 250 // Increased from 50 to 250 to lower Gmail API call rate
	csvFileBaseName       = "walmart_order_stats"
	htmlFileBaseName      = "walmart_order_report"
	fedexTokenURL         = "https://apis.fedex.com/oauth/token"
	fedexTrackingURL      = "https://apis.fedex.com/track/v1/trackingnumbers"
	shipmentDetailWorkers = 1 // Keep at 1 for now, can be tuned later
)

// ReportData (remains the same)
type ReportData struct {
	Timestamp              string
	Overall                OverallStatsData
	ProductStats           []ProductStatData
	ShippedOrders          []ShippedOrderData
	PendingShipmentOrders  []PendingShipmentData
	AwaitingShipmentOrders []AwaitingShipmentData
	EmailStats             EmailStatsDataContainer
}

// OverallStatsData (remains the same)
type OverallStatsData struct {
	TotalConfirmations int
	TotalCancellations int
	TotalNonCancelled  int
	TotalShipped       int
}

// ProductStatData (remains the same)
type ProductStatData struct {
	Name        string
	Confirmed   int
	NonCanceled int
	Shipped     int
	StickRate   float64
}

// ShippedOrderData (ShipmentStatus field was already added)
type ShippedOrderData struct {
	Email                       string
	OrderID                     string
	ProductName                 string
	TrackingNumber              string
	TrackingLink                string
	ShipmentStatus              string
	EstimatedDeliveryDate       string    // Will store the formatted, human-readable date
	parsedEstimatedDeliveryDate time.Time // For sorting
}

// PendingShipmentData (remains the same)
type PendingShipmentData struct {
	Email       string
	OrderID     string
	ProductName string
}

// AwaitingShipmentData (remains the same)
type AwaitingShipmentData struct {
	Email       string
	OrderID     string
	ProductName string
}

// EmailStatData (remains the same)
type EmailStatData struct {
	Email              string
	NonCanceled        int
	TotalCancellations int
}

// EmailStatsDataContainer (remains the same)
type EmailStatsDataContainer struct {
	TopNonCancelled   []EmailStatData
	OnlyCancellations []EmailStatData
}

// Order (remains the same)
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

// Struct for FedEx OAuth Token Response
type FedexOauthTokenResponse struct {
	AccessToken string `json:"access_token"`
	TokenType   string `json:"token_type"`
	ExpiresIn   int    `json:"expires_in"` // Typically in seconds
	Scope       string `json:"scope"`
}

// Structs for parsing FedEx Tracking API JSON output (adapted from FedexMcpResponse)
type TrackingDateAndTime struct {
	DateTime string `json:"dateTime"`
	Type     string `json:"type"`
}

type FedexTrackingApiResponse struct {
	TransactionID string `json:"transactionId"`
	Output        struct {
		CompleteTrackResults []struct {
			TrackResults []struct {
				LatestStatusDetail struct {
					StatusByLocale string `json:"statusByLocale"`
					DerivedStatus  string `json:"derivedStatus"`
					Description    string `json:"description"`
				} `json:"latestStatusDetail"`
				DateAndTimes []TrackingDateAndTime `json:"dateAndTimes"` // Added for estimated delivery date
				// We might need more fields later, but this is enough for status
			} `json:"trackResults"`
		} `json:"completeTrackResults"`
		Alerts []struct { // To capture API-level alerts/errors from FedEx
			Code    string `json:"code"`
			Message string `json:"message"`
		} `json:"alerts"`
	} `json:"output"`
	Errors []struct { // To capture top-level errors (e.g., auth errors if token is bad)
		Code    string `json:"code"`
		Message string `json:"message"`
	} `json:"errors"`
}

var (
	reOrderConfirmation = regexp.MustCompile(`(?i)thanks for your order|order confirmation`)
	reOrderCancellation = regexp.MustCompile(`(?i)Canceled:.*?order #([\d-]+)|your order.*?has been canceled`)
	reOrderShipped      = regexp.MustCompile(`(?i)Shipped:`)
	reOrderNumber       = regexp.MustCompile(`Order number:.*?(\d{7}-\d{8})`)
	reProductName       = regexp.MustCompile(`quantity \d+ item ([^"<]+)`)
	reTrackingNumber    = regexp.MustCompile(`(?i)tracking number <a[^>]*>([^<]+)</a>`)
)

func isFedexTrackingNumber(trackingNumber string) bool {
	trackingNumber = strings.TrimSpace(trackingNumber)
	if (len(trackingNumber) == 12 || len(trackingNumber) == 15 || len(trackingNumber) == 20 || len(trackingNumber) == 22) && regexp.MustCompile(`^\d+$`).MatchString(trackingNumber) {
		if strings.HasPrefix(trackingNumber, "96") ||
			strings.HasPrefix(trackingNumber, "6") ||
			strings.HasPrefix(trackingNumber, "7") ||
			strings.HasPrefix(trackingNumber, "56") ||
			len(trackingNumber) == 12 || len(trackingNumber) == 15 {
			return true
		}
	}
	return false
}

func getFedexAccessToken(clientID, clientSecret string) (string, error) {
	data := url.Values{}
	data.Set("grant_type", "client_credentials")
	data.Set("client_id", clientID)
	data.Set("client_secret", clientSecret)

	req, err := http.NewRequest("POST", fedexTokenURL, strings.NewReader(data.Encode()))
	if err != nil {
		return "", fmt.Errorf("error creating token request: %w", err)
	}
	req.Header.Add("Content-Type", "application/x-www-form-urlencoded")

	client := &http.Client{Timeout: 10 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		return "", fmt.Errorf("error fetching token: %w", err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", fmt.Errorf("error reading token response body: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("fedex token api error: %s - %s", resp.Status, string(body))
	}

	var tokenResponse FedexOauthTokenResponse
	if err := json.Unmarshal(body, &tokenResponse); err != nil {
		return "", fmt.Errorf("error unmarshalling token response: %w. Body: %s", err, string(body))
	}

	if tokenResponse.AccessToken == "" {
		return "", fmt.Errorf("received empty access token. Body: %s", string(body))
	}
	return tokenResponse.AccessToken, nil
}

// FedexShipmentDetails holds status, raw estimated delivery date string, and parsed time.Time object
type FedexShipmentDetails struct {
	Status                      string
	EstimatedDeliveryDate       string    // Raw date string from API
	ParsedEstimatedDeliveryDate time.Time // Parsed date for internal use/sorting
}

func getFedexShipmentDetails(trackingNumber, accessToken string) (FedexShipmentDetails, error) {
	details := FedexShipmentDetails{Status: "N/A", EstimatedDeliveryDate: "N/A", ParsedEstimatedDeliveryDate: time.Time{}} // Initialize ParsedEstimatedDeliveryDate to zero

	if !isFedexTrackingNumber(trackingNumber) {
		details.Status = "Non-FedEx / N/A"
		return details, nil // Not an error, just not a FedEx number
	}
	if accessToken == "" {
		details.Status = "Auth Error (No Token)"
		return details, fmt.Errorf("FedEx access token is empty")
	}

	payload := map[string]interface{}{
		"includeDetailedScans": true,
		"trackingInfo": []map[string]interface{}{
			{
				"trackingNumberInfo": map[string]string{
					"trackingNumber": trackingNumber,
				},
			},
		},
	}
	jsonPayload, err := json.Marshal(payload)
	if err != nil {
		log.Printf("Error marshalling FedEx payload for %s: %v", trackingNumber, err)
		details.Status = "Payload Error"
		return details, fmt.Errorf("error marshalling payload for %s: %w", trackingNumber, err)
	}

	req, err := http.NewRequest("POST", fedexTrackingURL, bytes.NewBuffer(jsonPayload))
	if err != nil {
		log.Printf("Error creating FedEx tracking request for %s: %v", trackingNumber, err)
		details.Status = "Request Creation Error"
		return details, fmt.Errorf("error creating request for %s: %w", trackingNumber, err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", "Bearer "+accessToken)

	client := &http.Client{Timeout: 15 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		log.Printf("Error calling FedEx tracking API for %s: %v", trackingNumber, err)
		details.Status = "API Call Error"
		return details, fmt.Errorf("error calling API for %s: %w", trackingNumber, err)
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		log.Printf("Error reading FedEx tracking response body for %s: %v", trackingNumber, err)
		details.Status = "Response Read Error"
		return details, fmt.Errorf("error reading response body for %s: %w", trackingNumber, err)
	}

	if resp.StatusCode != http.StatusOK {
		log.Printf("FedEx tracking API error for %s: %s. Body: %s", trackingNumber, resp.Status, string(body))
		var apiError FedexTrackingApiResponse
		if json.Unmarshal(body, &apiError) == nil && len(apiError.Errors) > 0 {
			details.Status = fmt.Sprintf("API Error: %s - %s", apiError.Errors[0].Code, apiError.Errors[0].Message)
			return details, fmt.Errorf("API error for %s: %s - %s", trackingNumber, apiError.Errors[0].Code, apiError.Errors[0].Message)
		}
		details.Status = fmt.Sprintf("API Error: %s", resp.Status)
		return details, fmt.Errorf("API error for %s: %s", trackingNumber, resp.Status)
	}

	var trackingResponse FedexTrackingApiResponse
	if err := json.Unmarshal(body, &trackingResponse); err != nil {
		log.Printf("Error unmarshalling FedEx tracking response for %s: %v. Raw: %s", trackingNumber, err, string(body))
		details.Status = "Parse Error (API)"
		return details, fmt.Errorf("error unmarshalling response for %s: %w. Raw: %s", trackingNumber, err, string(body))
	}

	if len(trackingResponse.Output.CompleteTrackResults) > 0 &&
		len(trackingResponse.Output.CompleteTrackResults[0].TrackResults) > 0 {
		trackResult := trackingResponse.Output.CompleteTrackResults[0].TrackResults[0]
		statusDetail := trackResult.LatestStatusDetail
		if statusDetail.StatusByLocale != "" {
			details.Status = statusDetail.StatusByLocale
		} else if statusDetail.DerivedStatus != "" {
			details.Status = statusDetail.DerivedStatus
		} else if statusDetail.Description != "" {
			details.Status = statusDetail.Description
		}

		for _, dt := range trackResult.DateAndTimes {
			if dt.Type == "ESTIMATED_DELIVERY" {
				details.EstimatedDeliveryDate = dt.DateTime // Store raw date string

				// Attempt to parse it for sorting
				layouts := []string{
					"2006-01-02T15:04:05-07:00",
					"2006-01-02T15:04:05Z",
					"2006-01-02T15:04:05",
					"2006-01-02",
				}
				parsed := false
				for _, layout := range layouts {
					parsedTime, err := time.Parse(layout, dt.DateTime)
					if err == nil {
						details.ParsedEstimatedDeliveryDate = parsedTime
						parsed = true
						break
					}
				}
				if !parsed {
					log.Printf("Could not parse estimated delivery date '%s' for tracking number %s", dt.DateTime, trackingNumber)
					details.ParsedEstimatedDeliveryDate = time.Time{} // Ensure it's zero if unparseable
				}
				break // Found the estimated delivery date
			}
		}
	} else if len(trackingResponse.Output.Alerts) > 0 {
		details.Status = fmt.Sprintf("Alert: %s", trackingResponse.Output.Alerts[0].Message)
	}

	return details, nil
}

func formatDeliveryDate(dateStr string) string {
	if dateStr == "" || dateStr == "N/A" {
		return "N/A"
	}

	// Try to parse the date string. FedEx might return just a date or a full timestamp.
	// Common formats: "YYYY-MM-DD", "YYYY-MM-DDTHH:MM:SS", "YYYY-MM-DDTHH:MM:SSZ", "YYYY-MM-DDTHH:MM:SS+HH:MM"
	layouts := []string{
		"2006-01-02T15:04:05-07:00", // Full timestamp with offset
		"2006-01-02T15:04:05Z",      // Full timestamp UTC
		"2006-01-02T15:04:05",       // Full timestamp without offset (assume local or UTC based on context)
		"2006-01-02",                // Date only
	}

	var deliveryTime time.Time
	var parseErr error
	parsed := false
	for _, layout := range layouts {
		deliveryTime, parseErr = time.Parse(layout, dateStr)
		if parseErr == nil {
			parsed = true
			break
		}
	}

	if !parsed {
		log.Printf("Error parsing date string '%s': %v. Returning original.", dateStr, parseErr)
		// Fallback to a simpler date format if direct parsing failed but it might be a simple date
		parts := strings.Split(dateStr, "T")
		if len(parts) > 0 {
			return parts[0] // Return YYYY-MM-DD part
		}
		return dateStr // Return original if all parsing fails
	}

	// Ensure we are comparing dates in the same location (e.g., local time)
	// For simplicity, let's use the server's local time for "today" and "tomorrow" comparisons.
	// FedEx dates might be UTC or local to the destination. For this formatting,
	// comparing date parts (Year, Month, Day) is usually sufficient.
	now := time.Now()
	today := time.Date(now.Year(), now.Month(), now.Day(), 0, 0, 0, 0, now.Location())
	tomorrow := today.AddDate(0, 0, 1)

	// Normalize deliveryTime to just date part for comparison
	deliveryDateOnly := time.Date(deliveryTime.Year(), deliveryTime.Month(), deliveryTime.Day(), 0, 0, 0, 0, now.Location())

	if deliveryDateOnly.Equal(today) {
		return "Arriving Today"
	}
	if deliveryDateOnly.Equal(tomorrow) {
		return "Arriving Tomorrow"
	}

	// For dates within the next week (but not today/tomorrow)
	nextWeek := today.AddDate(0, 0, 7)
	if deliveryDateOnly.After(tomorrow) && deliveryDateOnly.Before(nextWeek) {
		return "Arriving " + deliveryTime.Format("Monday, Jan 2") // e.g., Arriving Wednesday, May 21
	}

	// For other future dates or if it's a past date (though less likely for "estimated")
	// Could also check if deliveryTime is before today and format as "Delivered..." if status matches
	return "Arriving " + deliveryTime.Format("Mon, Jan 2, 2006") // e.g., Arriving May 22, 2025
}

func getTrackingLink(trackingNumber string) string {
	// (This function remains largely the same, isFedexTrackingNumber is now a separate helper)
	trackingNumber = strings.TrimSpace(trackingNumber)
	if trackingNumber == "" || trackingNumber == "N/A" {
		return ""
	}
	if (len(trackingNumber) >= 20 && len(trackingNumber) <= 22 && regexp.MustCompile(`^\d+$`).MatchString(trackingNumber)) ||
		(len(trackingNumber) == 13 && regexp.MustCompile(`^[A-Z]{2}\d{9}[A-Z]{2}$`).MatchString(trackingNumber)) ||
		(regexp.MustCompile(`^9[2-4]\d{18,20}$`).MatchString(trackingNumber)) {
		return fmt.Sprintf("https://tools.usps.com/go/TrackConfirmAction?tLabels=%s", trackingNumber)
	}
	if strings.HasPrefix(trackingNumber, "1Z") && len(trackingNumber) == 18 && regexp.MustCompile(`^1Z[0-9A-Z]{16}$`).MatchString(trackingNumber) {
		return fmt.Sprintf("https://www.ups.com/track?loc=en_US&tracknum=%s", trackingNumber)
	}
	if isFedexTrackingNumber(trackingNumber) {
		return fmt.Sprintf("https://www.fedex.com/fedextrack/?trknbr=%s", trackingNumber)
	}
	if strings.HasPrefix(trackingNumber, "TBA") && regexp.MustCompile(`^TBA[A-Z0-9]{12,15}$`).MatchString(trackingNumber) {
		return fmt.Sprintf("https://www.amazon.com/progress-tracker/package/%s", trackingNumber)
	}
	return ""
}

func main() {
	ctx := context.Background()

	// Load .env file
	err := godotenv.Load()
	if err != nil {
		log.Println("Warning: Error loading .env file, relying on environment variables")
	}

	fedexClientID := os.Getenv("FEDEX_CLIENT_ID")
	fedexClientSecret := os.Getenv("FEDEX_CLIENT_SECRET")

	if fedexClientID == "" || fedexClientSecret == "" {
		log.Fatalf("FEDEX_CLIENT_ID and FEDEX_CLIENT_SECRET environment variables must be set.")
	}

	log.Println("Fetching FedEx Access Token...")
	fedexAccessToken, err := getFedexAccessToken(fedexClientID, fedexClientSecret)
	if err != nil {
		log.Fatalf("Failed to get FedEx access token: %v", err)
	}
	log.Println("Successfully fetched FedEx Access Token.")

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

	reportData := generateReportData(orders, fedexAccessToken) // Pass token
	writeCSVStats(reportData)
	writeHTMLReport(reportData)
}

// Gmail related functions (initGmailService, getToken, etc.) remain unchanged
// ... (keep existing Gmail functions as they are) ...
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
		log.Printf("[Worker %d] Error getting message %s (attempt %d): %v", workerID, msgID, attempt+1, err)
		if strings.Contains(err.Error(), "rateLimitExceeded") || strings.Contains(err.Error(), "userRateLimitExceeded") || strings.Contains(err.Error(), "Quota exceeded") {
			sleepTime := baseRetrySleep * time.Duration(attempt+1)
			time.Sleep(sleepTime)
		} else {
			return Order{}, false
		}
	}
	if err != nil {
		log.Printf("[Worker %d] Failed to get message %s after %d attempts", workerID, msgID, maxRetryAttempts)
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
	order := Order{MsgID: msgID, Email: recipient, Subject: subject}
	if matches := reOrderCancellation.FindStringSubmatch(subject); len(matches) > 1 {
		order.IsCanceled = true
		order.OrderID = strings.ReplaceAll(matches[1], "-", "")
		log.Printf("[Worker %d] Found cancellation: Order %s for %s", workerID, order.OrderID, order.Email)
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
		if order.OrderID == "" {
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
		log.Printf("[Worker %d] Found shipment: Order %s, Product '%s', Tracking '%s' for %s", workerID, order.OrderID, order.ProductName, order.TrackingNumber, order.Email)
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
		log.Printf("[Worker %d] Found confirmation: Order %s, Product '%s' for %s", workerID, order.OrderID, order.ProductName, order.Email)
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

func generateReportData(orders []Order, fedexAccessToken string) ReportData { // Added fedexAccessToken param
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
	type shipmentDetailJob struct {
		Email          string
		OrderID        string
		ProductName    string
		TrackingNumber string
	}

	numShippedOrders := 0
	for _, orderMap := range shippedOrdersInfo {
		numShippedOrders += len(orderMap)
	}

	jobs := make(chan shipmentDetailJob, numShippedOrders)
	results := make(chan ShippedOrderData, numShippedOrders)
	var wgShipmentDetails sync.WaitGroup

	for w := 0; w < shipmentDetailWorkers; w++ {
		wgShipmentDetails.Add(1)
		go func() {
			defer wgShipmentDetails.Done()
			for job := range jobs {
				link := getTrackingLink(job.TrackingNumber)
				shipmentDetails := FedexShipmentDetails{Status: "N/A", EstimatedDeliveryDate: "N/A", ParsedEstimatedDeliveryDate: time.Time{}}
				var err error

				if isFedexTrackingNumber(job.TrackingNumber) {
					shipmentDetails, err = getFedexShipmentDetails(job.TrackingNumber, fedexAccessToken) // Pass token
					if err != nil {
						log.Printf("Error getting FedEx shipment details for %s: %v", job.TrackingNumber, err)
						// shipmentDetails.Status will contain an error message
					}
				}

				formattedDeliveryDate := formatDeliveryDate(shipmentDetails.EstimatedDeliveryDate)

				results <- ShippedOrderData{
					Email:                       job.Email,
					OrderID:                     job.OrderID,
					ProductName:                 job.ProductName,
					TrackingNumber:              job.TrackingNumber,
					TrackingLink:                link,
					ShipmentStatus:              shipmentDetails.Status,
					EstimatedDeliveryDate:       formattedDeliveryDate,                       // Use formatted date for display
					parsedEstimatedDeliveryDate: shipmentDetails.ParsedEstimatedDeliveryDate, // Use parsed date for sorting
				}
			}
		}()
	}

	for email, orderMap := range shippedOrdersInfo {
		for orderID, orderDetails := range orderMap {
			jobs <- shipmentDetailJob{
				Email:          email,
				OrderID:        orderID,
				ProductName:    orderDetails.ProductName,
				TrackingNumber: orderDetails.TrackingNumber,
			}
		}
	}
	close(jobs)

	go func() {
		wgShipmentDetails.Wait()
		close(results)
	}()

	for sod := range results {
		shippedOrdersData = append(shippedOrdersData, sod)
	}

	sort.Slice(shippedOrdersData, func(i, j int) bool {
		dateI := shippedOrdersData[i].parsedEstimatedDeliveryDate
		dateJ := shippedOrdersData[j].parsedEstimatedDeliveryDate

		isZeroI := dateI.IsZero()
		isZeroJ := dateJ.IsZero()

		if isZeroI && !isZeroJ { // N/A dates go last
			return false
		}
		if !isZeroI && isZeroJ { // Valid dates go first
			return true
		}
		if !isZeroI && !isZeroJ { // Both dates are valid, sort normally
			if !dateI.Equal(dateJ) {
				return dateI.Before(dateJ)
			}
		}
		// If dates are equal or both are N/A, sort by Email then OrderID
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

	var awaitingShipmentData []AwaitingShipmentData
	for email, orderMap := range confirmedByEmail {
		for orderID, productName := range orderMap {
			isCanceled := false
			if cancelMap, exists := canceledByEmail[email]; exists && cancelMap[orderID] {
				isCanceled = true
			}
			productNameFromConfirmation := productName
			shippedAsThisProduct := false
			if shippingDetails, shippingRecordExists := shippedOrdersInfo[email][orderID]; shippingRecordExists {
				effectiveShippedProductName := shippingDetails.ProductName
				if effectiveShippedProductName == "" || strings.HasPrefix(effectiveShippedProductName, "Unknown Product") {
					if productNameFromConfirmation != "" && !strings.HasPrefix(productNameFromConfirmation, "Unknown Product") {
						effectiveShippedProductName = productNameFromConfirmation
					}
				}
				if effectiveShippedProductName == productNameFromConfirmation {
					shippedAsThisProduct = true
				}
			}
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
		PendingShipmentOrders:  pendingShipmentData,
		AwaitingShipmentOrders: awaitingShipmentData,
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
	writePendingShipmentOrdersCSV(writer, data.PendingShipmentOrders)
	writeAwaitingShipmentOrdersCSV(writer, data.AwaitingShipmentOrders)
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
	writer.Write([]string{""}) // Add empty line for spacing
}

func writeProductStatsCSV(writer *csv.Writer, productStats []ProductStatData) {
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
	writer.Write([]string{"Shipped Orders Details"})
	writer.Write([]string{"Email", "Order ID", "Product Name", "Tracking Number", "Tracking Link", "Shipment Status", "Estimated Delivery Date"}) // Added Estimated Delivery Date
	for _, entry := range shippedOrders {
		writer.Write([]string{entry.Email, entry.OrderID, entry.ProductName, entry.TrackingNumber, entry.TrackingLink, entry.ShipmentStatus, entry.EstimatedDeliveryDate}) // Added entry.EstimatedDeliveryDate
	}
	writer.Write([]string{""})
}

func writePendingShipmentOrdersCSV(writer *csv.Writer, pendingOrders []PendingShipmentData) {
	writer.Write([]string{"Confirmed Orders - Pending Shipment (Not Canceled)"})
	writer.Write([]string{"Email", "Order ID", "Product Name"})
	for _, entry := range pendingOrders {
		writer.Write([]string{entry.Email, entry.OrderID, entry.ProductName})
	}
	writer.Write([]string{""})
}

func writeAwaitingShipmentOrdersCSV(writer *csv.Writer, awaitingOrders []AwaitingShipmentData) {
	writer.Write([]string{"Orders Not Cancelled & No Shipping Email Yet"})
	writer.Write([]string{"Email", "Order ID", "Product Name"})
	for _, entry := range awaitingOrders {
		writer.Write([]string{entry.Email, entry.OrderID, entry.ProductName})
	}
	writer.Write([]string{""})
}

func writeEmailStatsCSV(writer *csv.Writer, emailStats EmailStatsDataContainer) {
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
