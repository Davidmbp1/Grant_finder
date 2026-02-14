package main

import (
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strconv"
	"strings"
	"time"
)

type enrichResponse struct {
	Message              string         `json:"message"`
	Domain               string         `json:"domain"`
	OnlyMissingDeadlines bool           `json:"only_missing_deadlines"`
	BatchSizeUsed        int            `json:"batch_size_used"`
	MaxItems             int            `json:"max_items"`
	ConfidenceThreshold  float64        `json:"confidence_threshold"`
	ItemsScanned         int            `json:"items_scanned"`
	ItemsUpdated         int            `json:"items_updated"`
	PDFsParsed           int            `json:"pdfs_parsed"`
	DeadlinesAdded       int            `json:"deadlines_added"`
	StatusChanges        int            `json:"status_changes"`
	StatusUpdated        int            `json:"status_updated"`
	StatusCounts         map[string]int `json:"status_counts"`
}

type domainMetric struct {
	Domain         string
	DryRun         bool
	HTTPStatus     int
	Duration       time.Duration
	ItemsScanned   int
	ItemsUpdated   int
	PDFsParsed     int
	DeadlinesAdded int
	StatusChanges  int
	StatusUpdated  int
	Error          string
}

func main() {
	baseURL := flag.String("base-url", "http://localhost:8081", "API base URL")
	adminSecretFlag := flag.String("admin-secret", "", "Admin secret (or use ADMIN_SECRET env)")
	domainsCSV := flag.String("domains", "", "Comma-separated list of domains")
	domainsFile := flag.String("domains-file", "", "Path to file with one domain per line")
	onlyMissingDeadlines := flag.Bool("only-missing-deadlines", true, "Only enrich missing deadlines")
	batchSize := flag.Int("batch-size", 200, "Batch size per request")
	maxItems := flag.Int("max-items", 1000, "Max items per domain")
	confidenceThreshold := flag.Float64("confidence-threshold", 0.6, "Confidence threshold [0,1]")
	rateLimitMs := flag.Int("rate-limit-ms", 1000, "Delay between domain calls in milliseconds")
	timeoutSec := flag.Int("timeout-sec", 120, "HTTP timeout in seconds")
	dryRun := flag.Bool("dry-run", false, "Print planned calls only; do not execute")
	flag.Parse()

	adminSecret := strings.TrimSpace(*adminSecretFlag)
	if adminSecret == "" {
		adminSecret = strings.TrimSpace(os.Getenv("ADMIN_SECRET"))
	}
	if adminSecret == "" {
		exitErr(errors.New("missing admin secret: use -admin-secret or ADMIN_SECRET env"))
	}

	domains, err := loadDomains(*domainsCSV, *domainsFile)
	if err != nil {
		exitErr(err)
	}
	if len(domains) == 0 {
		exitErr(errors.New("no domains provided: use -domains or -domains-file"))
	}

	if *batchSize <= 0 || *maxItems <= 0 {
		exitErr(errors.New("batch-size and max-items must be > 0"))
	}
	if *confidenceThreshold < 0 || *confidenceThreshold > 1 {
		exitErr(errors.New("confidence-threshold must be between 0 and 1"))
	}
	if *timeoutSec <= 0 {
		exitErr(errors.New("timeout-sec must be > 0"))
	}

	client := &http.Client{Timeout: time.Duration(*timeoutSec) * time.Second}
	metrics := make([]domainMetric, 0, len(domains))

	for idx, domain := range domains {
		metric := domainMetric{Domain: domain, DryRun: *dryRun}
		start := time.Now()

		reqURL := buildURL(*baseURL, domain, *onlyMissingDeadlines, *batchSize, *maxItems, *confidenceThreshold)
		if *dryRun {
			metric.Duration = time.Since(start)
			fmt.Printf("[DRY-RUN] %s\n", reqURL)
			metrics = append(metrics, metric)
		} else {
			response, statusCode, callErr := callEnrich(client, reqURL, adminSecret)
			metric.Duration = time.Since(start)
			metric.HTTPStatus = statusCode
			if callErr != nil {
				metric.Error = callErr.Error()
			} else {
				metric.ItemsScanned = response.ItemsScanned
				metric.ItemsUpdated = response.ItemsUpdated
				metric.PDFsParsed = response.PDFsParsed
				metric.DeadlinesAdded = response.DeadlinesAdded
				metric.StatusChanges = response.StatusChanges
				metric.StatusUpdated = response.StatusUpdated
			}
			metrics = append(metrics, metric)
		}

		if idx < len(domains)-1 && *rateLimitMs > 0 {
			time.Sleep(time.Duration(*rateLimitMs) * time.Millisecond)
		}
	}

	printReport(metrics)
}

func loadDomains(csv, filePath string) ([]string, error) {
	set := map[string]struct{}{}

	for _, part := range strings.Split(csv, ",") {
		d := strings.TrimSpace(strings.ToLower(part))
		if d != "" {
			set[d] = struct{}{}
		}
	}

	if strings.TrimSpace(filePath) != "" {
		content, err := os.ReadFile(filePath)
		if err != nil {
			return nil, fmt.Errorf("failed to read domains-file: %w", err)
		}
		for _, line := range strings.Split(string(content), "\n") {
			d := strings.TrimSpace(strings.ToLower(line))
			if d == "" || strings.HasPrefix(d, "#") {
				continue
			}
			set[d] = struct{}{}
		}
	}

	domains := make([]string, 0, len(set))
	for d := range set {
		domains = append(domains, d)
	}
	sort.Strings(domains)
	return domains, nil
}

func buildURL(baseURL, domain string, onlyMissing bool, batchSize, maxItems int, confidence float64) string {
	u, _ := url.Parse(strings.TrimRight(baseURL, "/") + "/api/v1/admin/enrich-opportunities")
	q := u.Query()
	q.Set("domain", domain)
	q.Set("only_missing_deadlines", strconv.FormatBool(onlyMissing))
	q.Set("batch_size", strconv.Itoa(batchSize))
	q.Set("max_items", strconv.Itoa(maxItems))
	q.Set("confidence_threshold", strconv.FormatFloat(confidence, 'f', 2, 64))
	u.RawQuery = q.Encode()
	return u.String()
}

func callEnrich(client *http.Client, reqURL, adminSecret string) (*enrichResponse, int, error) {
	req, err := http.NewRequest(http.MethodPost, reqURL, nil)
	if err != nil {
		return nil, 0, err
	}
	req.Header.Set("X-Admin-Secret", adminSecret)

	resp, err := client.Do(req)
	if err != nil {
		return nil, 0, err
	}
	defer resp.Body.Close()

	var payload enrichResponse
	if err := json.NewDecoder(resp.Body).Decode(&payload); err != nil {
		return nil, resp.StatusCode, fmt.Errorf("decode failed: %w", err)
	}

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		if payload.Message == "" {
			return &payload, resp.StatusCode, fmt.Errorf("http %d", resp.StatusCode)
		}
		return &payload, resp.StatusCode, fmt.Errorf("http %d: %s", resp.StatusCode, payload.Message)
	}

	return &payload, resp.StatusCode, nil
}

func printReport(metrics []domainMetric) {
	fmt.Println("\n=== Enrichment Batch Report ===")
	fmt.Printf("%-28s %-6s %-6s %-8s %-8s %-8s %-10s %-8s %-8s %s\n",
		"domain", "dry", "http", "scanned", "updated", "pdfs", "deadlines", "changes", "sec", "error")

	totalScanned := 0
	totalUpdated := 0
	totalPDFs := 0
	totalDeadlines := 0
	totalChanges := 0
	errors := 0

	for _, m := range metrics {
		if m.Error != "" {
			errors++
		}
		totalScanned += m.ItemsScanned
		totalUpdated += m.ItemsUpdated
		totalPDFs += m.PDFsParsed
		totalDeadlines += m.DeadlinesAdded
		totalChanges += m.StatusChanges

		fmt.Printf("%-28s %-6t %-6d %-8d %-8d %-8d %-10d %-8d %-8.2f %s\n",
			m.Domain,
			m.DryRun,
			m.HTTPStatus,
			m.ItemsScanned,
			m.ItemsUpdated,
			m.PDFsParsed,
			m.DeadlinesAdded,
			m.StatusChanges,
			m.Duration.Seconds(),
			m.Error,
		)
	}

	fmt.Printf("\nTotals: scanned=%d updated=%d pdfs=%d deadlines_added=%d status_changes=%d errors=%d\n",
		totalScanned, totalUpdated, totalPDFs, totalDeadlines, totalChanges, errors)
}

func exitErr(err error) {
	fmt.Fprintf(os.Stderr, "error: %v\n", err)
	os.Exit(1)
}
