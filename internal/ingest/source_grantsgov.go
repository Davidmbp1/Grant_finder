package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strconv"
	"strings"
	"time"
)

// GrantsGovFetcher fetches opportunities from the Grants.gov search2 API.
type GrantsGovFetcher struct {
	Client  *http.Client
	BaseURL string
}

func NewGrantsGovFetcher() *GrantsGovFetcher {
	return &GrantsGovFetcher{
		Client: &http.Client{
			Timeout: 60 * time.Second,
		},
		BaseURL: "https://api.grants.gov/v1/api/search2",
	}
}

// GrantsGovSearchRequest matches the Grants.gov search2 API schema.
type GrantsGovSearchRequest struct {
	Keyword        string `json:"keyword"`
	OppStatuses    string `json:"oppStatuses"`
	SortBy         string `json:"sortBy"`
	Rows           int    `json:"rows"`
	StartRecordNum int    `json:"startRecordNum"`
}

// GrantsGovResponse represents the search2 API response (wrapped in "data").
type GrantsGovResponse struct {
	Data struct {
		HitCount    int               `json:"hitCount"`
		StartRecord int               `json:"startRecord"`
		OppHits     []GrantsGovRecord `json:"oppHits"`
	} `json:"data"`
	ErrorCode int    `json:"errorcode"`
	Msg       string `json:"msg"`
}

// GrantsGovRecord represents a single opportunity from Grants.gov
// with ALL available fields captured.
type GrantsGovRecord struct {
	ID         string   `json:"id"`
	Number     string   `json:"number"`
	Title      string   `json:"title"`
	Agency     string   `json:"agency"`
	AgencyCode string   `json:"agencyCode"`
	OpenDate   string   `json:"openDate"`
	CloseDate  string   `json:"closeDate"`
	OppStatus  string   `json:"oppStatus"`
	DocType    string   `json:"docType"`
	CFDAList   []string `json:"cfdaList"`
}

// FetchOpportunities fetches a page of opportunities from Grants.gov search2 API.
func (f *GrantsGovFetcher) FetchOpportunities(ctx context.Context, keyword string, rows, startRecord int) ([]Opportunity, int, error) {
	searchReq := GrantsGovSearchRequest{
		Keyword:        keyword,
		OppStatuses:    "posted",
		SortBy:         "openDate|desc",
		Rows:           rows,
		StartRecordNum: startRecord,
	}

	jsonBody, err := json.Marshal(searchReq)
	if err != nil {
		return nil, 0, fmt.Errorf("marshaling request: %w", err)
	}

	req, err := http.NewRequestWithContext(ctx, "POST", f.BaseURL, strings.NewReader(string(jsonBody)))
	if err != nil {
		return nil, 0, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json")

	log.Printf("[GrantsGov] Fetching page startRecord=%d rows=%d keyword=%q", startRecord, rows, keyword)

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, 0, fmt.Errorf("API request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		return nil, 0, fmt.Errorf("API returned %d: %s", resp.StatusCode, string(body))
	}

	var apiResp GrantsGovResponse
	if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
		return nil, 0, fmt.Errorf("decoding response: %w", err)
	}

	if apiResp.ErrorCode != 0 {
		return nil, 0, fmt.Errorf("API error: %s", apiResp.Msg)
	}

	log.Printf("[GrantsGov] Got %d opportunities (total: %d)", len(apiResp.Data.OppHits), apiResp.Data.HitCount)

	// Convert to our Opportunity type — capture ALL fields
	var opportunities []Opportunity
	for _, rec := range apiResp.Data.OppHits {
		opp := Opportunity{
			Title:             rec.Title,
			Summary:           fmt.Sprintf("Federal grant from %s. CFDA: %s", rec.Agency, strings.Join(rec.CFDAList, ", ")),
			ExternalURL:       fmt.Sprintf("https://www.grants.gov/search-results-detail/%s", rec.ID),
			SourceDomain:      "grants.gov",
			SourceID:          rec.ID,
			OpportunityNumber: rec.Number,
			AgencyName:        rec.Agency,
			AgencyCode:        rec.AgencyCode,
			FunderType:        "Government",
			DocType:           rec.DocType,
			CfdaList:          rec.CFDAList,
			OppStatus:         rec.OppStatus,
			Region:            "North America",
			Country:           "USA",
			Currency:          "USD",
			Category:          "other",
			Type:              "grant",
		}

		// Parse open date (MM/DD/YYYY)
		if rec.OpenDate != "" {
			if t, err := time.Parse("01/02/2006", rec.OpenDate); err == nil {
				opp.OpenDate = &t
			}
		}

		// Parse close date (MM/DD/YYYY)
		if rec.CloseDate != "" {
			opp.CloseDateRaw = rec.CloseDate
			if t, err := time.Parse("01/02/2006", rec.CloseDate); err == nil {
				// Skip if expired (compare against end of closing day in UTC)
				// CloseDate is strictly a date (00:00:00). We assume it expires at the end of that day.
				expiration := t.Add(24 * time.Hour)
				if expiration.Before(time.Now().UTC()) {
					continue
				}
				opp.DeadlineAt = &t
				opp.DeadlineStr = rec.CloseDate
			}
		}

		// Skip if no title
		if opp.Title == "" {
			continue
		}

		// Enrich with details
		// We ignore errors here to not fail the whole batch, just log or skip enrichment
		if details, err := f.FetchOpportunityDetails(ctx, rec.ID); err == nil {
			// The response structure usually has "synopsis" object
			if syn, ok := details["synopsis"].(map[string]interface{}); ok {
				if desc, ok := syn["synopsisDesc"].(string); ok && desc != "" {
					opp.Description = desc
				}
				if elig, ok := syn["applicantEligibilityDesc"].(string); ok {
					opp.Eligibility = []string{elig}
				}
				// Try to parse amounts if present in synopsis
				// They come as strings or numbers, need robust handling
				if ceiling, ok := syn["awardCeiling"].(string); ok && ceiling != "" {
					// Remove $ and ,
					clean := strings.ReplaceAll(strings.ReplaceAll(ceiling, "$", ""), ",", "")
					if val, err := strconv.ParseFloat(clean, 64); err == nil {
						opp.AmountMax = val
					}
				}
				if floor, ok := syn["awardFloor"].(string); ok && floor != "" {
					clean := strings.ReplaceAll(strings.ReplaceAll(floor, "$", ""), ",", "")
					if val, err := strconv.ParseFloat(clean, 64); err == nil {
						opp.AmountMin = val
					}
				}
			}
		} else {
			log.Printf("[GrantsGov] Failed to fetch details for %s: %v", rec.ID, err)
		}

		opportunities = append(opportunities, opp)
	}

	return opportunities, apiResp.Data.HitCount, nil
}

// FetchOpportunityDetails fetches detailed information for a specific opportunity ID
func (f *GrantsGovFetcher) FetchOpportunityDetails(ctx context.Context, oppID string) (map[string]interface{}, error) {
	url := "https://api.grants.gov/v1/api/fetchOpportunity"
	reqBody := map[string]string{"id": oppID}

	jsonBody, _ := json.Marshal(reqBody)
	req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var result map[string]interface{}
	if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
		return nil, err
	}
	return result, nil
}
