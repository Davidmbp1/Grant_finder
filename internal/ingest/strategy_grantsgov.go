package ingest

import (
	"context"
	"fmt"
	"log"
)

type GrantsGovStrategy struct{}

func (s *GrantsGovStrategy) Run(ctx context.Context, config SourceConfig, p *Pipeline) (IngestionStats, error) {
	stats := IngestionStats{}
	fetcher := NewGrantsGovFetcher()

	// Default to fetching all if not specified, or use schedule/config to limit?
	// For MVP of Registry, we fetch all open variants.
	// Grants.gov API uses "rows" and "startRecordNum"

	keyword := "" // fetch all
	pageSize := 25
	offset := 0

	// If we wanted to limit rounds, we could read from config.
	// For now, we replicate IngestGrantsGov logic: fetch until done.

	for {
		opportunities, totalHits, err := fetcher.FetchOpportunities(ctx, keyword, pageSize, offset)
		if err != nil {
			return stats, fmt.Errorf("grants.gov fetch error at offset %d: %w", offset, err)
		}

		stats.TotalFound = totalHits

		for _, opp := range opportunities {
			// Ensure source domain matches registry config if needed,
			// but GrantsGovFetcher already sets it to "grants.gov"

			if err := p.SaveOpportunity(ctx, opp); err != nil {
				log.Printf("[GrantsGov] Failed to save %q: %v", opp.Title, err)
				stats.Errors++
			} else {
				stats.TotalSaved++
			}
		}

		offset += len(opportunities)
		log.Printf("[GrantsGov] Progress: saved %d, fetched %d/%d", stats.TotalSaved, offset, totalHits)

		if len(opportunities) == 0 || offset >= totalHits {
			break
		}
	}

	return stats, nil
}
