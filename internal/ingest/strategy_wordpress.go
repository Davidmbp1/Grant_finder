package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/url"
	"strings"
	"time"
)

type WordPressStrategy struct{}

type wpPost struct {
	ID    int    `json:"id"`
	Date  string `json:"date"`
	Link  string `json:"link"`
	Title struct {
		Rendered string `json:"rendered"`
	} `json:"title"`
	Content struct {
		Rendered string `json:"rendered"`
	} `json:"content"`
	Excerpt struct {
		Rendered string `json:"rendered"`
	} `json:"excerpt"`
	// Status is usually "publish" for public posts
	Status string `json:"status"`
}

func (s *WordPressStrategy) Run(ctx context.Context, config SourceConfig, pipeline *Pipeline) (IngestionStats, error) {
	stats := IngestionStats{}

	// Determine API URL
	apiURL := config.BaseURL
	// Heuristic: if URL doesn't contain wp-json, append standard path
	if !strings.Contains(apiURL, "wp-json") {
		u, err := url.Parse(apiURL)
		if err != nil {
			return stats, fmt.Errorf("invalid base URL: %w", err)
		}
		// Avoid double slash
		base := strings.TrimRight(u.String(), "/")
		apiURL = base + "/wp-json/wp/v2/posts"
	}

	page := 1
	perPage := 20
	// Hard limit to prevent infinite loops (can be config driven later)
	maxPages := 5

	for {
		// Construct paginated URL
		pagedURL := fmt.Sprintf("%s?page=%d&per_page=%d", apiURL, page, perPage)

		doc, err := pipeline.Fetcher.Fetch(ctx, pagedURL)
		if err != nil {
			// Stop on 400/404 which usually indicates end of pagination
			if strings.Contains(err.Error(), "400") || strings.Contains(err.Error(), "404") {
				break
			}
			// Log and break for other errors to avoid crashing entire run
			fmt.Printf("Error fetching WP page %d: %v\n", page, err)
			break
		}

		bodyBytes, err := io.ReadAll(doc.Body)
		doc.Body.Close()
		if err != nil {
			return stats, fmt.Errorf("failed to read body: %w", err)
		}

		var posts []wpPost
		if err := json.Unmarshal(bodyBytes, &posts); err != nil {
			// If response is not array, it might be an error object or empty
			// Try to detect empty array []
			if string(bodyBytes) == "[]" {
				break
			}
			fmt.Printf("Failed to unmarshal WP response: %v\n", err)
			break
		}

		if len(posts) == 0 {
			break
		}

		stats.TotalFound += len(posts)

		for _, post := range posts {
			// Clean HTML from title and excerpt for better raw data quality
			cleanTitle := HTMLToText(post.Title.Rendered)
			cleanSummary := HTMLToText(post.Excerpt.Rendered)

			// Create RawOpportunity
			opp := RawOpportunity{
				Title: cleanTitle,
				// Description here acts as the FULL content (HTML is okay/expected here for detail)
				Description:  post.Content.Rendered,
				ExternalURL:  post.Link,
				SourceDomain: config.ID,
				SourceID:     fmt.Sprintf("%d", post.ID),
				Extra: map[string]string{
					"opp_status": "posted",
					"posted_at":  post.Date,
					"excerpt":    cleanSummary,
				},
			}

			// Prepend Excerpt if useful (as HTML) for the detail view
			if post.Excerpt.Rendered != "" {
				opp.Description = "<b>Summary:</b> " + post.Excerpt.Rendered + "<br/><hr><br/>" + opp.Description
			}

			// Save using pipeline.SaveRaw (handles normalization, deduplication, upsert)
			if err := pipeline.SaveRaw(ctx, opp); err != nil {
				stats.Errors++
				fmt.Printf("Failed to save WP post %d: %v\n", post.ID, err)
			} else {
				stats.TotalSaved++
			}
		}

		page++
		if page > maxPages {
			break
		}
	}

	return stats, nil
}

func parseWPDate(dateStr string) time.Time {
	// WordPress REST API default date format: YYYY-MM-DDTHH:MM:SS
	layout := "2006-01-02T15:04:05"
	t, err := time.Parse(layout, dateStr)
	if err != nil {
		return time.Now()
	}
	return t
}
