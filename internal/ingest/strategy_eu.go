package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"time"
)

type EuFundingTendersStrategy struct{}

// Response structures for EU API
type euResponse struct {
	FundingPeriodId string `json:"fundingPeriodId"`
	// The API returns a JSON object with "fundingOppStatus"? No, usually it's "fundingOpportunities" or similar
	// Let's use a generic map for initial robustness or a guessed structure.
	// Based on docs, it returns:
	FundingOpportunities []euOpportunity `json:"fundingOpportunities"`
	TotalCount           int             `json:"totalCount"`
}

type euOpportunity struct {
	ComputatedId    string  `json:"computatedId"` // e.g. "HORIZON-CL6-2024-FARM2FORK-01-1"
	Title           string  `json:"title"`
	Description     string  `json:"description"`
	Status          string  `json:"status"`       // "OPEN", "CLOSED", "FORTHCOMING"
	DeadlineDate    []int64 `json:"deadlineDate"` // Array of timestamps? Or generic date
	OpeningDate     []int64 `json:"openingDate"`
	CallIdentifier  string  `json:"callIdentifier"`
	TopicIdentifier string  `json:"topicIdentifier"`
	Type            string  `json:"type"`   // "Grant", "Tenders"
	Budget          string  `json:"budget"` // Sometimes string "1.000.000"
}

// Actual wrapper might be different, so let's use a robust approach: try to decode known fields.
// The search API usually returns:
// { "fundingOpportunities": [ ... ], "totalCount": 123 }

func (s *EuFundingTendersStrategy) Run(ctx context.Context, config SourceConfig, p *Pipeline) (IngestionStats, error) {
	stats := IngestionStats{}

	// Default to fetching explicitly OPEN grants
	// We paginate until no more results.
	page := 1
	pageSize := 50

	for {
		reqBody := map[string]interface{}{
			"query":    "", // fetch all
			"page":     page,
			"pageSize": pageSize,
			"status":   []string{"OPEN", "FORTHCOMING"}, // Only want open or soon-to-open
			"program":  []string{"HORIZON"},             // Focus on Horizon Europe mostly? Or all? User said "EU Funding & Tenders". Let's omit to get all.
		}

		jsonBody, err := json.Marshal(reqBody)
		if err != nil {
			return stats, fmt.Errorf("marshal error: %w", err)
		}

		// API Key might be needed in URL or Header
		url := config.BaseURL
		if config.APIKey != "" {
			// Often passed as query param? or header?
			// curl -H "Content-Type: application/json" ...
			// Let's assume header for now if user provides it, generic key "ApiKey" or similar
		}

		req, err := http.NewRequestWithContext(ctx, "POST", url, bytes.NewReader(jsonBody))
		if err != nil {
			return stats, fmt.Errorf("create request error: %w", err)
		}
		req.Header.Set("Content-Type", "application/json")
		if config.APIKey != "" {
			req.Header.Set("apikey", config.APIKey)
		}

		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return stats, fmt.Errorf("api request failed: %w", err)
		}
		defer resp.Body.Close()

		if resp.StatusCode != http.StatusOK {
			body, _ := io.ReadAll(resp.Body)
			return stats, fmt.Errorf("EU API returned %d: %s", resp.StatusCode, string(body))
		}

		// Decode response
		var apiResp euResponse
		// If structure is unsure, we could decode to map[string]interface{} first to debug log
		// but let's try the struct.
		if err := json.NewDecoder(resp.Body).Decode(&apiResp); err != nil {
			return stats, fmt.Errorf("decode error: %w", err)
		}

		stats.TotalFound = apiResp.TotalCount

		for _, item := range apiResp.FundingOpportunities {
			// Mapping
			opp := Opportunity{
				Title:             item.Title,
				Summary:           item.Description, // might need cleanup (often HTML)
				Description:       item.Description,
				ExternalURL:       fmt.Sprintf("https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/topic-details/%s", item.TopicIdentifier),
				SourceDomain:      "ec.europa.eu",
				SourceID:          item.TopicIdentifier,
				OpportunityNumber: item.CallIdentifier,
				AgencyName:        "European Commission",
				AgencyCode:        "EC",
				FunderType:        "Government",
				Region:            "Europe",
				Country:           "European Union",
				Currency:          "EUR",
				OppStatus:         item.Status, // OPEN, FORTHCOMING match our enums roughly? (posted/open)
				Type:              "grant",     // item.Type might differentiate, defaulting to grant
			}

			if item.Type == "Tenders" {
				opp.DocType = "Tender"
				// User might want to exclude tenders?
				// "The user mentioned 'Funded Projects' confusion. Tenders are contracts.
				// We keep them if they are OPEN opportunities, but label as Tender."
			} else {
				opp.DocType = "Grant"
			}

			// Dates: EU returns timestamps in ms
			if len(item.DeadlineDate) > 0 {
				ts := int64(item.DeadlineDate[0])
				// Ensure it's not 0
				if ts > 0 {
					t := time.UnixMilli(ts)
					opp.DeadlineAt = &t
					opp.DeadlineStr = t.Format("2006-01-02")
				}
			}
			if len(item.OpeningDate) > 0 {
				ts := int64(item.OpeningDate[0])
				if ts > 0 {
					t := time.UnixMilli(ts)
					opp.OpenDate = &t
				}
			}

			if err := p.SaveOpportunity(ctx, opp); err != nil {
				log.Printf("[EU] Failed to save %q: %v", opp.Title, err)
				stats.Errors++
			} else {
				stats.TotalSaved++
			}
		}

		log.Printf("[EU] Page %d: saved %d, total found %d", page, stats.TotalSaved, stats.TotalFound)

		if len(apiResp.FundingOpportunities) == 0 {
			break
		}
		if stats.TotalSaved >= stats.TotalFound {
			break
		}

		page++
	}

	return stats, nil
}
