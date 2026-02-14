package ingest

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/david/grant-finder/internal/ai"
	"github.com/david/grant-finder/internal/db"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/microcosm-cc/bluemonday"
	"github.com/pgvector/pgvector-go"
)

type Pipeline struct {
	DB      *pgxpool.Pool
	Store   *db.Store
	Fetcher Fetcher
	Parser  Parser
	AI      *ai.OllamaClient
}

func NewPipeline(pool *pgxpool.Pool, fetcher Fetcher, parser Parser, aiClient *ai.OllamaClient) *Pipeline {
	if fetcher == nil {
		// Default config for production
		config := FetchConfig{
			TimeoutSeconds: 30,
			MaxRetries:     3,
			RateLimitRPS:   2.0, // Polite but efficient
			AcceptLanguage: "en-US,en;q=0.9,es;q=0.8",
		}
		fetcher = NewRateLimitedFetcher(config)
	}
	return &Pipeline{
		DB:      pool,
		Store:   db.NewStore(pool),
		Fetcher: fetcher,
		Parser:  parser,
		AI:      aiClient,
	}
}

// Run fetches a URL, parses it with the LLM, and saves results.
func (p *Pipeline) Run(ctx context.Context, url string) error {
	log.Printf("Starting ingestion for: %s", url)

	// 1. Fetch
	doc, err := p.Fetcher.Fetch(ctx, url)
	if err != nil {
		return fmt.Errorf("fetch error: %w", err)
	}
	defer doc.Body.Close()

	// 2. Parse with LLM
	opportunities, err := p.Parser.Parse(ctx, doc.Body)
	if err != nil {
		return fmt.Errorf("parse error: %w", err)
	}

	// 3. Save
	saved := 0
	for _, opp := range opportunities {
		if opp.SourceDomain == "" {
			opp.SourceDomain = extractDomain(url)
		}
		if opp.ExternalURL == "" {
			opp.ExternalURL = url
		}
		if err := p.SaveOpportunity(ctx, opp); err != nil {
			log.Printf("Failed to save %q: %v", opp.Title, err)
		} else {
			saved++
			log.Printf("Saved: %s", opp.Title)
		}
	}

	log.Printf("Ingestion complete: %d/%d saved from %s", saved, len(opportunities), url)
	return nil
}

// IngestSource triggers ingestion for a specific source ID defined in registry.
func (p *Pipeline) IngestSource(ctx context.Context, sourceID string) (IngestionStats, error) {
	// 1. Create Run Record
	var runID string
	err := p.DB.QueryRow(ctx,
		"INSERT INTO ingest_runs (source_id, status) VALUES ($1, 'running') RETURNING run_id",
		sourceID).Scan(&runID)
	if err != nil {
		log.Printf("[Warn] Failed to create ingest run: %v", err)
	} else {
		// Attach runID to context for SaveOpportunity to pick up
		ctx = context.WithValue(ctx, "source_run_id", runID)
	}

	start := time.Now()
	stats := IngestionStats{}

	defer func() {
		// Update run record on exit
		duration := time.Since(start)
		status := "completed"
		if stats.Errors > 0 {
			// Threshold can be adjusted. existing logic: if errors, marked as completed with errors?
			// or 'failed' if totally failed?
			// For now, if errors > 0 but some saved, it's 'completed' (with warnings).
			// If errors == total items?
			if stats.TotalSaved == 0 && stats.TotalFound > 0 {
				status = "failed"
			}
		}

		if runID != "" {
			_, execErr := p.DB.Exec(ctx,
				`UPDATE ingest_runs SET 
					status = $1, 
					items_found = $2, 
					items_saved = $3, 
					errors = $4, 
					completed_at = NOW(),
					details = $5
				WHERE run_id = $6`,
				status, stats.TotalFound, stats.TotalSaved, stats.Errors,
				fmt.Sprintf(`{"duration_ms": %d}`, duration.Milliseconds()),
				runID,
			)
			if execErr != nil {
				log.Printf("Failed to update ingest run %s: %v", runID, execErr)
			}
		}
	}()

	// Load registry (in production, this might be loaded once at startup)
	registry, err := LoadRegistry("internal/config/sources.yaml")
	if err != nil {
		return IngestionStats{}, fmt.Errorf("failed to load registry: %w", err)
	}

	var config *SourceConfig
	for _, src := range registry.Sources {
		if src.ID == sourceID {
			config = &src
			break
		}
	}

	if config == nil {
		return IngestionStats{}, fmt.Errorf("source id %q not found in registry", sourceID)
	}

	strategy, err := GlobalStrategyFactory.Get(config.Strategy)
	if err != nil {
		return IngestionStats{}, fmt.Errorf("strategy %q not found for source %q", config.Strategy, sourceID)
	}

	log.Printf("Starting ingestion for source: %s (%s)", config.Name, config.ID)
	// Update stats variable with result
	s, err := strategy.Run(ctx, *config, p)
	stats = s // capture stats for defer
	return stats, err
}

// IngestAll triggers ingestion for ALL sources in the registry.
func (p *Pipeline) IngestAll(ctx context.Context) (map[string]IngestionStats, error) {
	registry, err := LoadRegistry("internal/config/sources.yaml")
	if err != nil {
		return nil, fmt.Errorf("failed to load registry: %w", err)
	}

	results := make(map[string]IngestionStats)

	for _, src := range registry.Sources {
		stats, err := p.IngestSource(ctx, src.ID)
		if err != nil {
			log.Printf("Error ingesting source %q: %v", src.ID, err)
			// We continue with other sources
			results[src.ID] = IngestionStats{Errors: 1} // Mark as error
		} else {
			results[src.ID] = stats
		}
	}

	return results, nil
}

// SaveRaw normalizes a raw opportunity and saves it to the database.
func (p *Pipeline) SaveRaw(ctx context.Context, raw RawOpportunity) error {
	opp := FromRaw(raw)
	return p.SaveOpportunity(ctx, opp)
}

func (p *Pipeline) SaveOpportunity(ctx context.Context, opp Opportunity) error {
	// 1. Normalize Data (Clean countries, funder types, text)
	NormalizeOpportunity(&opp)

	// Sanitize all text fields to remove invalid UTF-8 sequences and strip HTML from summary/title
	opp.Title = sanitizeUTF8(HTMLToText(opp.Title))
	opp.Summary = sanitizeUTF8(HTMLToText(opp.Summary))

	// Fallback: If summary is empty (or was just HTML tags), derive from description
	if strings.TrimSpace(opp.Summary) == "" && strings.TrimSpace(opp.Description) != "" {
		// Description might be HTML, so clean it first
		cleanDesc := HTMLToText(opp.Description)
		opp.Summary = TruncateText(cleanDesc, 280)
	}

	// Sanitize HTML description (remove scripts, unsafe tags)
	opp.Description = sanitizeHTML(sanitizeUTF8(opp.Description))

	// 2. Conditional LLM Extraction (Augmentation)
	// Logic: If critical fields are missing (Deadline), check DB first. If still missing, use LLM.
	needsExtraction := false
	if opp.DeadlineAt == nil && !opp.IsRolling {
		needsExtraction = true
	}

	if needsExtraction {
		// Optimization: Check DB first to avoid expensive LLM calls if we already have the data
		existing, err := p.Store.GetOpportunityBySourceID(ctx, opp.SourceDomain, opp.SourceID)
		if err == nil && existing != nil {
			// Copy hard-won data from existing record
			if existing.DeadlineAt != nil {
				opp.DeadlineAt = existing.DeadlineAt
				needsExtraction = false
			}
			if existing.IsRolling {
				opp.IsRolling = true
				needsExtraction = false
			}
			// If existing status is effectively closed/archived, stop trying to extract
			if existing.OppStatus == "closed" || existing.OppStatus == "archived" {
				needsExtraction = false
			}
		}

		// If still needs extraction and AI is available
		if needsExtraction && p.AI != nil {
			log.Printf("🤖 Triggering LLM extraction for %q (Source: %s)", opp.Title, opp.SourceID)

			// Prepare text context (limited length)
			textCtx := fmt.Sprintf("%s\n%s", opp.Summary, HTMLToText(opp.Description))
			if len(textCtx) > 8000 {
				textCtx = textCtx[:8000]
			}

			extracted, err := p.AI.ExtractOpportunityData(ctx, opp.Title, opp.ExternalURL, textCtx)
			if err != nil {
				log.Printf("⚠️ LLM extraction failed: %v", err)
			} else {
				// Merge extracted data
				if extracted.SourceStatusRaw != "" {
					opp.SourceStatusRaw = extracted.SourceStatusRaw
				}
				if extracted.IsResultsPage {
					opp.IsResultsPage = true
				}
				if len(extracted.DeadlineCandidates) > 0 {
					opp.Deadlines = mergeUniqueFold(opp.Deadlines, extracted.DeadlineCandidates)
				}
				if extracted.DeadlineISO != "" {
					if dt, err := time.Parse("2006-01-02", extracted.DeadlineISO); err == nil {
						// Set properly to end of day in UTC
						dt = time.Date(dt.Year(), dt.Month(), dt.Day(), 23, 59, 59, 999000000, time.UTC)
						opp.DeadlineAt = &dt
						opp.Deadlines = mergeUniqueFold(opp.Deadlines, []string{dt.Format(time.RFC3339)})
					}
				}
				if extracted.OpenISO != "" {
					if dt, ok := parseDeadlineCandidate(extracted.OpenISO); ok {
						opp.OpenAt = &dt
					}
				} else if extracted.OpenDateISO != "" {
					if dt, ok := parseDeadlineCandidate(extracted.OpenDateISO); ok {
						opp.OpenAt = &dt
					}
				}
				if extracted.CloseISO != "" {
					if dt, ok := parseDeadlineCandidate(extracted.CloseISO); ok {
						opp.CloseAt = &dt
					}
				}
				if extracted.ExpirationISO != "" {
					if dt, ok := parseDeadlineCandidate(extracted.ExpirationISO); ok {
						opp.ExpirationAt = &dt
					}
				}
				if extracted.IsRolling {
					opp.IsRolling = true
				}
				if extracted.OppStatus != "" && (extracted.OppStatus == "posted" || extracted.OppStatus == "closed" || extracted.OppStatus == "archived" || extracted.OppStatus == "funded") {
					opp.OppStatus = extracted.OppStatus
					if opp.SourceStatusRaw == "" {
						opp.SourceStatusRaw = extracted.OppStatus
					}
				}
				if extracted.AmountMin > 0 {
					opp.AmountMin = extracted.AmountMin
				}
				if extracted.AmountMax > 0 {
					opp.AmountMax = extracted.AmountMax
				}
				if extracted.Currency != "" {
					opp.Currency = extracted.Currency
				}

				// MERGE Missing Metadata
				// Summary: Only if missing or very short
				if (opp.Summary == "" || len(opp.Summary) < 40) && extracted.Summary != "" {
					opp.Summary = extracted.Summary
				}
				// Categories: Merge unique
				if len(extracted.Categories) > 0 {
					opp.Categories = mergeUniqueFold(opp.Categories, extracted.Categories)
					if len(opp.Categories) > 6 {
						opp.Categories = opp.Categories[:6]
					}
				}
				// Eligibility: Merge unique
				if extracted.Eligibility != "" {
					opp.Eligibility = mergeUniqueFold(opp.Eligibility, splitAndCleanList(extracted.Eligibility))
				}
			}
		}
	}

	// Set defaults for production fields
	if opp.SourceRunID == "" {
		if runID, ok := ctx.Value("source_run_id").(string); ok {
			opp.SourceRunID = runID
		}
	}
	if opp.CanonicalURL == "" && opp.ExternalURL != "" {
		opp.CanonicalURL = CanonicalizeURL(opp.ExternalURL)
	}
	if opp.ContentType == "" {
		opp.ContentType = "html"
	}
	if opp.RawURL == "" {
		opp.RawURL = opp.ExternalURL
	}

	// Generate embedding if missing
	if len(opp.Embedding) == 0 && p.AI != nil {
		text := fmt.Sprintf("%s\n%s", opp.Title, opp.Summary)
		if len(text) > 8000 {
			text = text[:8000]
		}
		vec, err := p.AI.GenerateEmbedding(ctx, text)
		if err != nil {
			log.Printf("⚠️ Failed to generate embedding for %q: %v", opp.Title, err)
		} else {
			opp.Embedding = vec
		}
	}

	if strings.TrimSpace(opp.SourceID) == "" {
		return fmt.Errorf("missing source_id (url=%s, source=%s)", opp.ExternalURL, opp.SourceDomain)
	}

	if shouldEnrichEvidence(opp) && opp.ExternalURL != "" {
		_ = p.applyEvidenceEnrichment(ctx, &opp)
	}
	opp.RollingEvidence = detectRollingEvidence(opp)

	statusDecision := ComputeStatusDecision(opp, time.Now().UTC())
	opp.NormalizedStatus = statusDecision.NormalizedStatus
	opp.StatusReason = statusDecision.StatusReason
	opp.StatusConfidence = statusDecision.StatusConfidence
	opp.NextDeadlineAt = statusDecision.NextDeadlineAt
	opp.IsResultsPage = statusDecision.IsResultsPage

	if opp.SourceStatusRaw == "" {
		opp.SourceStatusRaw = opp.OppStatus
	}
	if opp.OpenAt == nil && opp.OpenDate != nil {
		opp.OpenAt = opp.OpenDate
	}
	if opp.SourceEvidenceJSON == nil {
		opp.SourceEvidenceJSON = map[string]interface{}{}
	}
	if isAPIFirstSource(opp.SourceDomain) && (opp.OpenAt != nil || opp.CloseAt != nil || opp.ExpirationAt != nil || len(opp.Deadlines) > 0) {
		opp.SourceEvidenceJSON["authority"] = "api"
		if opp.StatusConfidence < 0.95 {
			opp.StatusConfidence = 0.95
		}
	}
	if !opp.RollingEvidence {
		opp.IsRolling = false
	}

	deadlinesJSON := buildDeadlinesJSON(opp.Deadlines, opp.DeadlineEvidence, opp.ExternalURL)
	evidenceJSON := buildEvidenceJSON(opp.SourceEvidenceJSON)

	query := `
		INSERT INTO opportunities (
			title, summary, description_html, external_url, source_domain,
			source_id, opportunity_number, agency_name, agency_code, funder_type,
			amount_min, amount_max, currency, deadline_at, open_date,
			is_rolling, doc_type, cfda_list, opp_status, close_date_raw,
			region, country, categories, eligibility, embedding,
			source_run_id, canonical_url, raw_url, content_type, data_quality_score,
			source_status_raw, normalized_status, status_reason, next_deadline_at,
			expiration_at, close_at, open_at, deadlines, is_results_page,
			source_evidence_json, status_confidence, rolling_evidence
		) VALUES (
			$1, $2, $3, $4, $5,
			$6, $7, $8, $9, $10,
			$11, $12, $13, $14, $15,
			$16, $17, $18, $19, $20,
			$21, $22, $23, $24, $25,
			$26, $27, $28, $29, $30,
			$31, $32, $33, $34,
			$35, $36, $37, $38::jsonb, $39,
			$40::jsonb, $41, $42
		)
		ON CONFLICT (source_domain, source_id) DO UPDATE SET
			updated_at = NOW(),
			title = EXCLUDED.title,
			summary = EXCLUDED.summary,
			description_html = COALESCE(NULLIF(EXCLUDED.description_html, ''), opportunities.description_html),
			deadline_at = COALESCE(EXCLUDED.deadline_at, opportunities.deadline_at),
			amount_min = COALESCE(NULLIF(EXCLUDED.amount_min, 0), opportunities.amount_min),
			amount_max = COALESCE(NULLIF(EXCLUDED.amount_max, 0), opportunities.amount_max),
			currency = COALESCE(NULLIF(EXCLUDED.currency, ''), opportunities.currency),
			open_date = COALESCE(EXCLUDED.open_date, opportunities.open_date),
			close_date_raw = COALESCE(NULLIF(EXCLUDED.close_date_raw, ''), opportunities.close_date_raw),
			doc_type = COALESCE(NULLIF(EXCLUDED.doc_type, ''), opportunities.doc_type),
			opp_status = CASE 
				-- Prevent re-opening if currently closed/archived/funded and new status is weak (posted or empty)
				WHEN opportunities.opp_status IN ('closed', 'archived', 'funded') AND COALESCE(EXCLUDED.opp_status, 'posted') IN ('posted', '') THEN opportunities.opp_status 
				ELSE COALESCE(NULLIF(EXCLUDED.opp_status, ''), opportunities.opp_status) 
			END,
			is_rolling = COALESCE(opportunities.is_rolling, false) OR COALESCE(EXCLUDED.is_rolling, false),
			opportunity_number = COALESCE(NULLIF(EXCLUDED.opportunity_number, ''), opportunities.opportunity_number),
			categories = COALESCE(NULLIF(EXCLUDED.categories, '{}'::text[]), opportunities.categories),
			eligibility = COALESCE(NULLIF(EXCLUDED.eligibility, '{}'::text[]), opportunities.eligibility),
			cfda_list = COALESCE(NULLIF(EXCLUDED.cfda_list, '{}'::text[]), opportunities.cfda_list),
			embedding = COALESCE(EXCLUDED.embedding, opportunities.embedding),
			source_run_id = EXCLUDED.source_run_id,
			canonical_url = EXCLUDED.canonical_url,
			raw_url = EXCLUDED.raw_url,
			content_type = EXCLUDED.content_type,
			data_quality_score = EXCLUDED.data_quality_score,
			source_status_raw = COALESCE(NULLIF(EXCLUDED.source_status_raw, ''), opportunities.source_status_raw),
			normalized_status = EXCLUDED.normalized_status,
			status_reason = EXCLUDED.status_reason,
			next_deadline_at = EXCLUDED.next_deadline_at,
			expiration_at = COALESCE(EXCLUDED.expiration_at, opportunities.expiration_at),
			close_at = COALESCE(EXCLUDED.close_at, opportunities.close_at),
			open_at = COALESCE(EXCLUDED.open_at, opportunities.open_at),
			deadlines = COALESCE(EXCLUDED.deadlines, opportunities.deadlines),
			is_results_page = EXCLUDED.is_results_page,
			source_evidence_json = COALESCE(EXCLUDED.source_evidence_json, opportunities.source_evidence_json),
			status_confidence = GREATEST(COALESCE(EXCLUDED.status_confidence, 0), COALESCE(opportunities.status_confidence, 0)),
			rolling_evidence = COALESCE(EXCLUDED.rolling_evidence, opportunities.rolling_evidence)
	`

	var embedding interface{}
	if len(opp.Embedding) > 0 {
		embedding = pgvector.NewVector(opp.Embedding)
	}

	_, err := p.DB.Exec(ctx, query,
		opp.Title,                         // $1
		opp.Summary,                       // $2
		opp.Description,                   // $3
		opp.ExternalURL,                   // $4
		nilIfEmpty(opp.SourceDomain),      // $5
		opp.SourceID,                      // $6 (Strictly non-null)
		nilIfEmpty(opp.OpportunityNumber), // $7
		nilIfEmpty(opp.AgencyName),        // $8
		nilIfEmpty(opp.AgencyCode),        // $9
		nilIfEmpty(opp.FunderType),        // $10
		opp.AmountMin,                     // $11
		opp.AmountMax,                     // $12
		opp.Currency,                      // $13
		opp.DeadlineAt,                    // $14
		opp.OpenDate,                      // $15
		opp.IsRolling,                     // $16
		nilIfEmpty(opp.DocType),           // $17
		opp.CfdaList,                      // $18
		nilIfEmpty(opp.OppStatus),         // $19
		nilIfEmpty(opp.CloseDateRaw),      // $20
		nilIfEmpty(opp.Region),            // $21
		nilIfEmpty(opp.Country),           // $22
		opp.Categories,                    // $23
		opp.Eligibility,                   // $24
		embedding,                         // $25
		nilIfEmpty(opp.SourceRunID),       // $26
		opp.CanonicalURL,                  // $27
		opp.RawURL,                        // $28
		opp.ContentType,                   // $29
		opp.DataQualityScore,              // $30
		nilIfEmpty(opp.SourceStatusRaw),   // $31
		opp.NormalizedStatus,              // $32
		nilIfEmpty(opp.StatusReason),      // $33
		opp.NextDeadlineAt,                // $34
		opp.ExpirationAt,                  // $35
		opp.CloseAt,                       // $36
		opp.OpenAt,                        // $37
		deadlinesJSON,                     // $38
		opp.IsResultsPage,                // $39
		evidenceJSON,                     // $40
		opp.StatusConfidence,             // $41
		opp.RollingEvidence,              // $42
	)
	return err
}

func buildDeadlinesJSON(deadlines []string, evidence []DeadlineEvidence, fallbackURL string) interface{} {
	merged := mergeDeadlineEvidence(evidence, deadlines, fallbackURL)
	if len(merged) == 0 {
		return nil
	}

	payload, err := json.Marshal(merged)
	if err != nil {
		return nil
	}

	return string(payload)
}

func mergeDeadlineEvidence(existing []DeadlineEvidence, legacyDates []string, fallbackURL string) []DeadlineEvidence {
	out := make([]DeadlineEvidence, 0, len(existing)+len(legacyDates))
	seen := map[string]bool{}
	for _, ev := range existing {
		iso := strings.TrimSpace(ev.ParsedDateISO)
		if iso == "" {
			continue
		}
		if seen[iso] {
			continue
		}
		if ev.Source == "" {
			ev.Source = "html"
		}
		if ev.URL == "" {
			ev.URL = fallbackURL
		}
		if ev.Confidence == 0 {
			ev.Confidence = 0.7
		}
		out = append(out, ev)
		seen[iso] = true
	}
	for _, raw := range legacyDates {
		if dt, ok := parseDeadlineCandidate(raw); ok {
			iso := dt.UTC().Format(time.RFC3339)
			if seen[iso] {
				continue
			}
			out = append(out, DeadlineEvidence{
				Source:        "legacy",
				URL:           fallbackURL,
				Snippet:       raw,
				ParsedDateISO: iso,
				Label:         "legacy_deadline",
				Confidence:    0.5,
			})
			seen[iso] = true
		}
	}
	return out
}

func decodeDeadlinesPayload(raw []byte) ([]string, []DeadlineEvidence) {
	if len(raw) == 0 {
		return nil, nil
	}

	var evidence []DeadlineEvidence
	if err := json.Unmarshal(raw, &evidence); err == nil {
		dates := make([]string, 0, len(evidence))
		for _, ev := range evidence {
			if ev.ParsedDateISO != "" {
				dates = append(dates, ev.ParsedDateISO)
			}
		}
		return mergeUniqueFold(nil, dates), evidence
	}

	var dates []string
	if err := json.Unmarshal(raw, &dates); err == nil {
		return mergeUniqueFold(nil, dates), mergeDeadlineEvidence(nil, dates, "")
	}

	return nil, nil
}

func buildEvidenceJSON(evidence map[string]interface{}) interface{} {
	if len(evidence) == 0 {
		return nil
	}

	payload, err := json.Marshal(evidence)
	if err != nil {
		return nil
	}

	return string(payload)
}

// nilIfEmpty returns nil for empty strings so NULL is stored in DB.
func nilIfEmpty(s string) interface{} {
	if s == "" {
		return nil
	}
	return s
}

// sanitizeUTF8 removes invalid UTF-8 byte sequences that cause PostgreSQL errors.
func sanitizeUTF8(s string) string {
	if utf8.ValidString(s) {
		return s
	}
	v := strings.ToValidUTF8(s, "")
	return v
}

// sanitizeHTML uses bluemonday to strip unsafe tags and attributes from HTML.
func sanitizeHTML(s string) string {
	// create a policy (UGCPolicy allows images, links, tables, etc. but removes scripts/iframes)
	p := bluemonday.UGCPolicy()
	return p.Sanitize(s)
}

func extractDomain(url string) string {
	u := url
	if idx := findSubstring(u, "://"); idx >= 0 {
		u = u[idx+3:]
	}
	if idx := findSubstring(u, "/"); idx >= 0 {
		u = u[:idx]
	}
	return u
}

func findSubstring(s, substr string) int {
	for i := 0; i <= len(s)-len(substr); i++ {
		if s[i:i+len(substr)] == substr {
			return i
		}
	}
	return -1
}

// RefineAllData iterates over all existing opportunities, applies normalization, and re-saves them.
func (p *Pipeline) RefineAllData(ctx context.Context) (int, error) {
	// 1. Fetch all IDs
	rows, err := p.DB.Query(ctx, "SELECT id FROM opportunities")
	if err != nil {
		return 0, fmt.Errorf("failed to fetch IDs: %w", err)
	}
	defer rows.Close()

	var ids []string
	for rows.Next() {
		var id string
		if err := rows.Scan(&id); err == nil {
			ids = append(ids, id)
		}
	}
	rows.Close()

	log.Printf("Refining %d records...", len(ids))
	updated := 0

	// 2. Query all necessary fields directly
	// We scan directly into variables to populate ingest.Opportunity
	// Note: We need description and embedding which are not in models.Opportunity
	query := "SELECT title, summary, COALESCE(description_html, ''), external_url, source_domain, source_id, opportunity_number, agency_name, agency_code, funder_type, amount_min, amount_max, currency, deadline_at, open_date, is_rolling, doc_type, cfda_list, opp_status, region, country, categories, eligibility, close_date_raw FROM opportunities WHERE id = $1"

	for _, id := range ids {
		var opp Opportunity
		var sourceID, oppNum, agencyName, agencyCode, funderType, docType, oppStatus, region, country, closeDateRaw *string

		err := p.DB.QueryRow(ctx, query, id).Scan(
			&opp.Title, &opp.Summary, &opp.Description, &opp.ExternalURL, &opp.SourceDomain,
			&sourceID, &oppNum, &agencyName, &agencyCode, &funderType,
			&opp.AmountMin, &opp.AmountMax, &opp.Currency, &opp.DeadlineAt, &opp.OpenDate,
			&opp.IsRolling, &docType, &opp.CfdaList, &oppStatus,
			&region, &country, &opp.Categories, &opp.Eligibility, &closeDateRaw,
		)

		if err != nil {
			log.Printf("Skipping %s (scan error): %v", id, err)
			continue
		}

		// Handle nullable strings
		if sourceID != nil {
			opp.SourceID = *sourceID
		}
		if oppNum != nil {
			opp.OpportunityNumber = *oppNum
		}
		if agencyName != nil {
			opp.AgencyName = *agencyName
		}
		if agencyCode != nil {
			opp.AgencyCode = *agencyCode
		}
		if funderType != nil {
			opp.FunderType = *funderType
		}
		if docType != nil {
			opp.DocType = *docType
		}
		if oppStatus != nil {
			opp.OppStatus = *oppStatus
		}
		if region != nil {
			opp.Region = *region
		}
		if country != nil {
			opp.Country = *country
		}
		if closeDateRaw != nil {
			opp.CloseDateRaw = *closeDateRaw
		}

		// AI Status Refinement
		p.refineGrantStatus(ctx, &opp)

		// saveOpportunity will call NormalizeOpportunity internally
		if err := p.SaveOpportunity(ctx, opp); err != nil {
			log.Printf("Failed to update %s: %v", id, err)
		} else {
			updated++
		}

		if updated%100 == 0 {
			log.Printf("Refined %d/%d", updated, len(ids))
		}
	}

	return updated, nil
}

// RefineGrantStatus uses LLM to check status if ambiguous
func (p *Pipeline) refineGrantStatus(ctx context.Context, opp *Opportunity) {
	// 0. Explicitly close if deadline passed (trust the data)
	if opp.DeadlineAt != nil && opp.DeadlineAt.Before(time.Now()) {
		opp.OppStatus = "closed"
		return // No need for AI
	}

	// Only refine if:
	// 1. Status is "posted" (default)
	// 2. No future deadline
	// 3. Not rolling (rolling usually implies open)
	// 4. We have an AI client
	if opp.OppStatus == "posted" &&
		(opp.DeadlineAt == nil || opp.DeadlineAt.Before(time.Now())) &&
		!opp.IsRolling &&
		p.AI != nil {

		log.Printf("Analyzing status for ambiguous grant: %s", opp.Title)
		// Use Description if available, otherwise Summary
		textToAnalyze := opp.Description
		if textToAnalyze == "" {
			textToAnalyze = opp.Summary
		}
		status, err := ai.AnalyzeStatus(ctx, p.AI, opp.Title, textToAnalyze)
		if err == nil && status != "posted" {
			opp.OppStatus = status
			log.Printf("[Auto-Status] AI determined status for %q as: %s", opp.Title, status)
		}
	}
}

func (p *Pipeline) RecomputeStatuses(ctx context.Context, batchSize int) (map[string]int, int, error) {
	if batchSize <= 0 {
		batchSize = 500
	}

	updated := 0
	counts := map[string]int{}
	lastID := ""

	for {
		rows, err := p.DB.Query(ctx, `
			SELECT id::text, title, COALESCE(summary,''), COALESCE(description_html,''), external_url,
			       is_rolling, rolling_evidence, COALESCE(opp_status,''), COALESCE(source_status_raw,''),
			       deadline_at, next_deadline_at, expiration_at, close_at, open_at,
			       COALESCE(deadlines, '[]'::jsonb), is_results_page,
			       COALESCE(source_evidence_json, '{}'::jsonb)
			FROM opportunities
			WHERE ($1 = '' OR id::text > $1)
			ORDER BY id::text
			LIMIT $2
		`, lastID, batchSize)
		if err != nil {
			return counts, updated, fmt.Errorf("recompute status query failed: %w", err)
		}

		batchRows := 0
		for rows.Next() {
			batchRows++
			var id string
			var opp Opportunity
			var deadlinesRaw []byte
			var evidenceRaw []byte

			if err := rows.Scan(
				&id, &opp.Title, &opp.Summary, &opp.Description, &opp.ExternalURL,
				&opp.IsRolling, &opp.RollingEvidence, &opp.OppStatus, &opp.SourceStatusRaw,
				&opp.DeadlineAt, &opp.NextDeadlineAt, &opp.ExpirationAt, &opp.CloseAt, &opp.OpenAt,
				&deadlinesRaw, &opp.IsResultsPage, &evidenceRaw,
			); err != nil {
				rows.Close()
				return counts, updated, fmt.Errorf("recompute status scan failed: %w", err)
			}

			opp.Deadlines, opp.DeadlineEvidence = decodeDeadlinesPayload(deadlinesRaw)
			if len(evidenceRaw) > 0 {
				_ = json.Unmarshal(evidenceRaw, &opp.SourceEvidenceJSON)
			}

			// Override stored is_results_page: let the engine re-derive it
			// from current detection logic (stored value may be stale/wrong).
			opp.IsResultsPage = false

			decision := ComputeStatusDecision(opp, time.Now().UTC())

			// LLM fallback: if the rule engine can't decide (needs_review),
			// use the LLM to classify the grant status.
			if decision.NormalizedStatus == "needs_review" && p.AI != nil {
				llmCtx, llmCancel := context.WithTimeout(ctx, 60*time.Second)
				llmStatus, llmErr := ai.AnalyzeStatus(llmCtx, p.AI, opp.Title, opp.Summary)
				llmCancel()
				if llmErr == nil && llmStatus != "" {
					switch llmStatus {
					case "posted":
						decision.NormalizedStatus = "open"
						decision.StatusReason = "llm_classified_open"
						decision.StatusConfidence = 0.6
					case "closed":
						decision.NormalizedStatus = "closed"
						decision.StatusReason = "llm_classified_closed"
						decision.StatusConfidence = 0.6
					case "forthcoming":
						decision.NormalizedStatus = "upcoming"
						decision.StatusReason = "llm_classified_upcoming"
						decision.StatusConfidence = 0.6
					}
				} else if llmErr != nil {
					log.Printf("[recompute] LLM classify failed for %s: %v", id, llmErr)
				}
			}

			rollingEvidence := detectRollingEvidence(opp)
			normalizedCloseAt := opp.CloseAt
			if opp.CloseAt != nil && !opp.CloseAt.After(time.Now().UTC()) && decision.NextDeadlineAt != nil && decision.NextDeadlineAt.After(time.Now().UTC()) {
				normalizedCloseAt = nil
			}

			tag, err := p.DB.Exec(ctx, `
				UPDATE opportunities
				SET normalized_status = $1::normalized_status_enum,
				    status_reason = $2,
				    next_deadline_at = $3,
				    is_results_page = $4,
				    status_confidence = $5,
				    rolling_evidence = $6,
				    close_at = $7
				WHERE id = $8
				  AND (
				      normalized_status::text IS DISTINCT FROM $1
				      OR status_reason IS DISTINCT FROM $2
				      OR next_deadline_at IS DISTINCT FROM $3
				      OR is_results_page IS DISTINCT FROM $4
				      OR status_confidence IS DISTINCT FROM $5
				      OR rolling_evidence IS DISTINCT FROM $6
				      OR close_at IS DISTINCT FROM $7
				  )
			`, decision.NormalizedStatus, nilIfEmpty(decision.StatusReason), decision.NextDeadlineAt, decision.IsResultsPage, decision.StatusConfidence, rollingEvidence, normalizedCloseAt, id)
			if err != nil {
				rows.Close()
				return counts, updated, fmt.Errorf("recompute status update failed: %w", err)
			}

			if tag.RowsAffected() > 0 {
				updated++
			}
			counts[decision.NormalizedStatus]++
			lastID = id
		}
		rows.Close()

		if batchRows == 0 {
			break
		}
	}

	return counts, updated, nil
}

func (p *Pipeline) BackfillCleanArrays(ctx context.Context) (int, error) {
	row := p.DB.QueryRow(ctx, `
		WITH cleaned AS (
			SELECT o.id,
				(
					SELECT COALESCE(array_agg(d.cleaned ORDER BY d.ord), '{}'::text[])
					FROM (
						SELECT DISTINCT ON (lower(x.cleaned)) x.cleaned, x.ord
						FROM (
							SELECT
								btrim(regexp_replace(regexp_replace(e, '^\s*[[:punct:]]+\s*', '', 'g'), '^\s*[0-9]+\s*[^[:alnum:]]*\s*', '', 'g')) AS cleaned,
								ord
							FROM unnest(COALESCE(o.eligibility,'{}'::text[])) WITH ORDINALITY AS t(e,ord)
						) x
						WHERE x.cleaned <> ''
						ORDER BY lower(x.cleaned), x.ord
					) d
				) AS eligibility_clean,
				(
					SELECT COALESCE(array_agg(d.cleaned ORDER BY d.ord), '{}'::text[])
					FROM (
						SELECT DISTINCT ON (lower(x.cleaned)) x.cleaned, x.ord
						FROM (
							SELECT
								btrim(regexp_replace(regexp_replace(c, '^\s*[[:punct:]]+\s*', '', 'g'), '^\s*[0-9]+\s*[^[:alnum:]]*\s*', '', 'g')) AS cleaned,
								ord
							FROM unnest(COALESCE(o.categories,'{}'::text[])) WITH ORDINALITY AS t(c,ord)
						) x
						WHERE x.cleaned <> ''
						ORDER BY lower(x.cleaned), x.ord
					) d
				) AS categories_clean
			FROM opportunities o
		), upd AS (
			UPDATE opportunities o
			SET eligibility = c.eligibility_clean,
				categories = c.categories_clean
			FROM cleaned c
			WHERE o.id = c.id
			  AND (o.eligibility IS DISTINCT FROM c.eligibility_clean OR o.categories IS DISTINCT FROM c.categories_clean)
			RETURNING o.id
		)
		SELECT COUNT(*) FROM upd
	`)

	var updated int
	if err := row.Scan(&updated); err != nil {
		return 0, fmt.Errorf("backfill clean arrays failed: %w", err)
	}

	return updated, nil
}

func shouldEnrichEvidence(opp Opportunity) bool {
	return !opp.RollingEvidence && opp.NextDeadlineAt == nil && opp.CloseAt == nil && opp.DeadlineAt == nil
}

func isAPIFirstSource(domain string) bool {
	d := strings.ToLower(strings.TrimSpace(domain))
	apiDomains := []string{"grants.gov", "api.grants.gov", "ec.europa.eu", "europa.eu", "nsf.gov", "nih.gov"}
	for _, candidate := range apiDomains {
		if strings.Contains(d, candidate) {
			return true
		}
	}
	return false
}

func (p *Pipeline) applyEvidenceEnrichment(ctx context.Context, opp *Opportunity) error {
	adapter := NewGenericSourceAdapter(p.Fetcher)
	raw, err := adapter.FetchOpportunityRaw(ctx, opp.ExternalURL)
	if err != nil {
		return err
	}

	candidates, err := adapter.ExtractCandidates(raw)
	if err != nil {
		return err
	}

	if candidates.SourceStatusRaw != "" {
		opp.SourceStatusRaw = candidates.SourceStatusRaw
	}
	if candidates.IsResultsPage {
		opp.IsResultsPage = true
	}
	if len(candidates.DeadlineCandidates) > 0 {
		opp.Deadlines = mergeUniqueFold(opp.Deadlines, candidates.DeadlineCandidates)
		opp.DeadlineEvidence = mergeDeadlineEvidence(opp.DeadlineEvidence, nil, opp.ExternalURL)
		opp.DeadlineEvidence = append(opp.DeadlineEvidence, candidates.DeadlineEvidence...)
		for _, ev := range candidates.DeadlineEvidence {
			parsed, ok := parseDeadlineCandidate(ev.ParsedDateISO)
			if !ok {
				continue
			}
			label := strings.ToLower(ev.Label + " " + ev.Snippet)
			if (strings.Contains(label, "inicio") || strings.Contains(label, "opening") || strings.Contains(label, "open")) && opp.OpenAt == nil {
				t := parsed.UTC()
				opp.OpenAt = &t
			}
			if strings.Contains(label, "cierre") || strings.Contains(label, "deadline") || strings.Contains(label, "closes") || strings.Contains(label, "fecha máxima") {
				if opp.CloseAt == nil || parsed.UTC().Before(*opp.CloseAt) {
					t := parsed.UTC()
					opp.CloseAt = &t
				}
			}
		}
		if opp.DeadlineAt == nil {
			if parsed, ok := parseDeadlineCandidate(candidates.DeadlineCandidates[0]); ok {
				opp.DeadlineAt = &parsed
			}
		}
	}
	if opp.SourceEvidenceJSON == nil {
		opp.SourceEvidenceJSON = map[string]interface{}{}
	}
	for k, v := range candidates.Evidence {
		opp.SourceEvidenceJSON[k] = v
	}
	opp.SourceEvidenceJSON["pdfs_parsed"] = candidates.PDFsParsed
	opp.SourceEvidenceJSON["deadlines_added"] = candidates.DeadlinesAdded
	if candidates.RollingEvidence {
		opp.IsRolling = true
		opp.RollingEvidence = true
		opp.SourceEvidenceJSON["rolling_evidence"] = true
	}
	if candidates.StatusConfidence > opp.StatusConfidence {
		opp.StatusConfidence = candidates.StatusConfidence
	}

	return nil
}

type EnrichmentStats struct {
	ItemsScanned  int `json:"items_scanned"`
	ItemsUpdated  int `json:"items_updated"`
	PDFsParsed    int `json:"pdfs_parsed"`
	DeadlinesAdded int `json:"deadlines_added"`
	StatusChanges int `json:"status_changes"`
}

func (p *Pipeline) EnrichOpportunities(ctx context.Context, domain string, onlyMissingDeadlines bool, batchSize int, maxItems int, confidenceThreshold float64) (EnrichmentStats, error) {
	stats := EnrichmentStats{}
	if batchSize <= 0 {
		batchSize = 200
	}
	if maxItems <= 0 {
		maxItems = batchSize
	}
	if confidenceThreshold <= 0 {
		confidenceThreshold = 0.6
	}
	ttlInterval := domainTTLIntervalLiteral(domain)

	query := `
		SELECT id::text, title, COALESCE(summary,''), COALESCE(description_html,''), external_url,
		       source_domain, source_id, is_rolling, rolling_evidence, COALESCE(opp_status,''), COALESCE(source_status_raw,''),
		       normalized_status::text, COALESCE(status_reason,''),
		       deadline_at, next_deadline_at, close_at, expiration_at, COALESCE(deadlines, '[]'::jsonb),
		       COALESCE(source_evidence_json, '{}'::jsonb), COALESCE(status_confidence, 0)
		FROM opportunities
		WHERE ($1 = '' OR source_domain = $1)
		  AND (
				(normalized_status IN ('open', 'needs_review') AND next_deadline_at IS NULL AND rolling_evidence = false)
				OR COALESCE(status_reason,'') IN ('rolling_without_evidence', 'missing_deadline', 'inconsistent_dates')
				OR COALESCE(status_confidence, 0) < $2
				OR COALESCE(last_enriched_at, 'epoch'::timestamptz) < NOW() - $4::interval
			  )
		ORDER BY updated_at ASC
		LIMIT $3
	`

	if !onlyMissingDeadlines {
		query = `
			SELECT id::text, title, COALESCE(summary,''), COALESCE(description_html,''), external_url,
			       source_domain, source_id, is_rolling, rolling_evidence, COALESCE(opp_status,''), COALESCE(source_status_raw,''),
			       normalized_status::text, COALESCE(status_reason,''),
			       deadline_at, next_deadline_at, close_at, expiration_at, COALESCE(deadlines, '[]'::jsonb),
			       COALESCE(source_evidence_json, '{}'::jsonb), COALESCE(status_confidence, 0)
			FROM opportunities
			WHERE ($1 = '' OR source_domain = $1)
			  AND (
					normalized_status IN ('open', 'needs_review')
					OR COALESCE(status_reason,'') IN ('rolling_without_evidence', 'missing_deadline', 'inconsistent_dates')
					OR COALESCE(status_confidence, 0) < $2
					OR COALESCE(last_enriched_at, 'epoch'::timestamptz) < NOW() - $4::interval
				  )
			ORDER BY updated_at ASC
			LIMIT $3
		`
	}

	rows, err := p.DB.Query(ctx, query, domain, confidenceThreshold, batchSize, ttlInterval)
	if err != nil {
		return stats, fmt.Errorf("enrichment query failed: %w", err)
	}
	defer rows.Close()

	processed := 0
	updated := 0
	for rows.Next() {
		if processed >= maxItems {
			break
		}
		processed++
		var id string
		var opp Opportunity
		var deadlinesRaw []byte
		var evidenceRaw []byte
		var previousStatus string
		var previousReason string

		if err := rows.Scan(
			&id, &opp.Title, &opp.Summary, &opp.Description, &opp.ExternalURL,
			&opp.SourceDomain, &opp.SourceID, &opp.IsRolling, &opp.RollingEvidence, &opp.OppStatus, &opp.SourceStatusRaw,
			&previousStatus, &previousReason,
			&opp.DeadlineAt, &opp.NextDeadlineAt, &opp.CloseAt, &opp.ExpirationAt, &deadlinesRaw,
			&evidenceRaw, &opp.StatusConfidence,
		); err != nil {
			return stats, fmt.Errorf("enrichment scan failed: %w", err)
		}

		opp.Deadlines, opp.DeadlineEvidence = decodeDeadlinesPayload(deadlinesRaw)
		if len(evidenceRaw) > 0 {
			_ = json.Unmarshal(evidenceRaw, &opp.SourceEvidenceJSON)
		}
		beforeCount := len(opp.DeadlineEvidence)

		_ = p.applyEvidenceEnrichment(ctx, &opp)
		opp.RollingEvidence = detectRollingEvidence(opp)
		if !opp.RollingEvidence {
			opp.IsRolling = false
		}
		stats.DeadlinesAdded += max(0, len(opp.DeadlineEvidence)-beforeCount)
		if pdfCount, ok := opp.SourceEvidenceJSON["pdfs_parsed"].(int); ok {
			stats.PDFsParsed += pdfCount
		}
		if pdfCountFloat, ok := opp.SourceEvidenceJSON["pdfs_parsed"].(float64); ok {
			stats.PDFsParsed += int(pdfCountFloat)
		}
		decision := ComputeStatusDecision(opp, time.Now().UTC())
		fetchStatusCode, fetchBytes, fetchDurationMs, fetchBlocked := extractFetchMeta(opp.SourceEvidenceJSON)
		if previousStatus != decision.NormalizedStatus || previousReason != decision.StatusReason {
			stats.StatusChanges++
		}

		tag, err := p.DB.Exec(ctx, `
			UPDATE opportunities
			SET source_status_raw = COALESCE(NULLIF($1,''), source_status_raw),
			    deadlines = COALESCE($2::jsonb, deadlines),
			    next_deadline_at = $3,
			    close_at = COALESCE($4, close_at),
			    expiration_at = COALESCE($5, expiration_at),
			    is_rolling = $6,
			    rolling_evidence = $7,
			    is_results_page = $8,
			    source_evidence_json = COALESCE($9::jsonb, source_evidence_json),
			    normalized_status = $10::normalized_status_enum,
			    status_reason = $11,
			    status_confidence = GREATEST($12::double precision, $13::double precision),
			    last_enriched_at = NOW(),
			    fetch_last_status_code = COALESCE($14, fetch_last_status_code),
			    fetch_last_bytes = COALESCE($15, fetch_last_bytes),
			    fetch_last_duration_ms = COALESCE($16, fetch_last_duration_ms),
			    fetch_blocked_detected = COALESCE($17, fetch_blocked_detected)
			WHERE id = $18
		`, opp.SourceStatusRaw, buildDeadlinesJSON(opp.Deadlines, opp.DeadlineEvidence, opp.ExternalURL), decision.NextDeadlineAt, opp.CloseAt, opp.ExpirationAt,
			opp.IsRolling, opp.RollingEvidence, decision.IsResultsPage, buildEvidenceJSON(opp.SourceEvidenceJSON), decision.NormalizedStatus, nilIfEmpty(decision.StatusReason), decision.StatusConfidence, opp.StatusConfidence, fetchStatusCode, fetchBytes, fetchDurationMs, fetchBlocked, id)
		if err != nil {
			return stats, fmt.Errorf("enrichment update failed: %w", err)
		}
		if tag.RowsAffected() > 0 {
			updated++
		}
	}

	if err := rows.Err(); err != nil {
		return stats, fmt.Errorf("enrichment iteration failed: %w", err)
	}

	stats.ItemsScanned = processed
	stats.ItemsUpdated = updated
	return stats, nil
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

func domainTTLIntervalLiteral(domain string) string {
	d := strings.ToLower(strings.TrimSpace(domain))
	if strings.Contains(d, "gob.pe") || strings.Contains(d, "proinnovate") || strings.Contains(d, "prociencia") {
		return "48 hours"
	}
	if strings.Contains(d, "ukri") || strings.Contains(d, "neh") {
		return "72 hours"
	}
	return "168 hours"
}

func extractFetchMeta(evidence map[string]interface{}) (*int, *int, *int, *bool) {
	if len(evidence) == 0 {
		return nil, nil, nil, nil
	}
	fetchRaw, ok := evidence["fetch_meta"]
	if !ok {
		return nil, nil, nil, nil
	}

	fetchMap, ok := fetchRaw.(map[string]interface{})
	if !ok {
		return nil, nil, nil, nil
	}

	toIntPtr := func(value interface{}) *int {
		switch typed := value.(type) {
		case int:
			v := typed
			return &v
		case float64:
			v := int(typed)
			return &v
		default:
			return nil
		}
	}

	toBoolPtr := func(value interface{}) *bool {
		switch typed := value.(type) {
		case bool:
			v := typed
			return &v
		default:
			return nil
		}
	}

	statusCode := toIntPtr(fetchMap["root_status_code"])
	bytes := toIntPtr(fetchMap["root_bytes"])
	durationMs := toIntPtr(fetchMap["root_duration_ms"])
	blocked := toBoolPtr(fetchMap["blocked_detected"])
	if blocked == nil {
		blocked = toBoolPtr(fetchMap["pdf_unparseable"])
	}

	return statusCode, bytes, durationMs, blocked
}

