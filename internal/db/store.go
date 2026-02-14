package db

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/david/grant-finder/internal/models"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pgvector/pgvector-go"
)

type Store struct {
	pool *pgxpool.Pool
}

func NewStore(pool *pgxpool.Pool) *Store {
	return &Store{pool: pool}
}

type ListParams struct {
	Query          string
	QueryEmbedding []float32
	Source         string
	MinAmount      float64
	MaxAmount      float64
	DeadlineDays   int
	IsRolling      *bool
	Limit          int
	Offset         int
	Categories     []string
	Eligibility    []string
	Region         []string
	FunderType     []string
	Country        []string
	AgencyCode     string
	AgencyName     []string
	SortBy         string
	Status         string // "posted" (default), "closed", "archived", "forthcoming", "needs_review", or "all"
	ExcludeExpired bool   // Deprecated: use Status filter instead
}

type ListResult struct {
	Opportunities []models.Opportunity `json:"opportunities"`
	Total         int                  `json:"total"`
	Limit         int                  `json:"limit"`
	Offset        int                  `json:"offset"`
}

// selectCols is the comprehensive column list for all queries.
const selectCols = `id, title, summary, external_url, source_domain,
	source_id, opportunity_number, agency_name, agency_code, funder_type,
	amount_min, amount_max, currency, deadline_at, next_deadline_at, open_date, open_at, close_at, expiration_at,
	is_rolling, rolling_evidence, doc_type, cfda_list, opp_status, source_status_raw, normalized_status, status_reason, deadlines, is_results_page,
	source_evidence_json, status_confidence,
	region, country, categories, eligibility, created_at`

func scanOpportunity(scan func(dest ...interface{}) error) (models.Opportunity, error) {
	var o models.Opportunity
	var summary, sourceID, oppNum, agencyName, agencyCode, funderType *string
	var docType, oppStatus, sourceStatusRaw, normalizedStatus, statusReason, region, country *string
	var deadlinesRaw []byte
	var evidenceRaw []byte

	err := scan(
		&o.ID, &o.Title, &summary, &o.ExternalURL, &o.SourceDomain,
		&sourceID, &oppNum, &agencyName, &agencyCode, &funderType,
		&o.AmountMin, &o.AmountMax, &o.Currency, &o.DeadlineAt, &o.NextDeadlineAt, &o.OpenDate, &o.OpenAt, &o.CloseAt, &o.ExpirationAt,
		&o.IsRolling, &o.RollingEvidence, &docType, &o.CfdaList, &oppStatus, &sourceStatusRaw, &normalizedStatus, &statusReason, &deadlinesRaw, &o.IsResultsPage,
		&evidenceRaw, &o.StatusConfidence,
		&region, &country, &o.Categories, &o.Eligibility, &o.CreatedAt,
	)
	if err != nil {
		return o, err
	}

	// Assign nullable strings
	if summary != nil {
		o.Summary = *summary
	}
	if sourceID != nil {
		o.SourceID = *sourceID
	}
	if oppNum != nil {
		o.OpportunityNumber = *oppNum
	}
	if agencyName != nil {
		o.AgencyName = *agencyName
	}
	if agencyCode != nil {
		o.AgencyCode = *agencyCode
	}
	if funderType != nil {
		o.FunderType = *funderType
	}
	if docType != nil {
		o.DocType = *docType
	}
	if oppStatus != nil {
		o.OppStatus = *oppStatus
	}
	if sourceStatusRaw != nil {
		o.SourceStatusRaw = *sourceStatusRaw
	}
	if normalizedStatus != nil {
		o.NormalizedStatus = *normalizedStatus
	}
	if statusReason != nil {
		o.StatusReason = *statusReason
	}
	if len(deadlinesRaw) > 0 {
		o.Deadlines = decodeDeadlineDates(deadlinesRaw)
	}
	if len(evidenceRaw) > 0 {
		_ = json.Unmarshal(evidenceRaw, &o.SourceEvidenceJSON)
	}
	if region != nil {
		o.Region = *region
	}
	if country != nil {
		o.Country = *country
	}

	return o, nil
}

func (s *Store) ListOpportunities(ctx context.Context, params ListParams) (*ListResult, error) {
	// 1. Build WHERE clause and Args
	where := "WHERE 1=1"
	var args []interface{}
	argIdx := 1

	// Hybrid Search / Scoring
	if params.Query != "" {
		where += fmt.Sprintf(" AND (search_vector @@ plainto_tsquery('english', $%d) OR title ILIKE '%%' || $%d || '%%')", argIdx, argIdx)
		args = append(args, params.Query)
		argIdx++
	}

	if params.Source != "" {
		where += fmt.Sprintf(" AND source_domain = $%d", argIdx)
		args = append(args, params.Source)
		argIdx++
	}
	if len(params.Region) > 0 {
		where += fmt.Sprintf(" AND region = ANY($%d)", argIdx)
		args = append(args, params.Region)
		argIdx++
	}
	if len(params.FunderType) > 0 {
		where += fmt.Sprintf(" AND funder_type = ANY($%d)", argIdx)
		args = append(args, params.FunderType)
		argIdx++
	}
	if len(params.Country) > 0 {
		where += fmt.Sprintf(" AND country = ANY($%d)", argIdx)
		args = append(args, params.Country)
		argIdx++
	}
	if params.AgencyCode != "" {
		where += fmt.Sprintf(" AND agency_code = $%d", argIdx)
		args = append(args, params.AgencyCode)
		argIdx++
	}
	if len(params.AgencyName) > 0 {
		where += fmt.Sprintf(" AND agency_name = ANY($%d)", argIdx)
		args = append(args, params.AgencyName)
		argIdx++
	}
	if params.MinAmount > 0 {
		where += fmt.Sprintf(" AND amount_max >= $%d", argIdx)
		args = append(args, params.MinAmount)
		argIdx++
	}
	if params.MaxAmount > 0 {
		where += fmt.Sprintf(" AND amount_min <= $%d", argIdx)
		args = append(args, params.MaxAmount)
		argIdx++
	}
	// Status Filter logic on normalized_status.
	targetStatus := params.Status
	if targetStatus == "" {
		targetStatus = "open"
	}

	if targetStatus == "active" {
		targetStatus = "open"
	}

	if targetStatus == "open" {
		where += buildOpenTabConstraint()
	} else if targetStatus == "all" {
		// No filter.
	} else if targetStatus == "closed" {
		where += " AND normalized_status::text IN ('closed','archived')"
	} else {
		// Support product statuses: open, upcoming, closed, archived, needs_review.
		if targetStatus == "posted" {
			targetStatus = "open"
		}

		where += fmt.Sprintf(" AND normalized_status::text = $%d", argIdx)
		args = append(args, targetStatus)
		argIdx++
	}

	// Deadline days filter (if specified, overrides default expired filter for deadline)
	if params.DeadlineDays > 0 {
		where += fmt.Sprintf(`
			AND (
				is_rolling = true
				OR (next_deadline_at IS NOT NULL AND next_deadline_at >= NOW() AND next_deadline_at <= NOW() + ($%d * INTERVAL '1 day'))
			)
		`, argIdx)
		args = append(args, params.DeadlineDays)
		argIdx++
	}

	if params.IsRolling != nil {
		where += fmt.Sprintf(" AND is_rolling = $%d", argIdx)
		args = append(args, *params.IsRolling)
		argIdx++
	}

	if len(params.Categories) > 0 {
		params.Categories = sanitizeStringSlice(params.Categories)
	}
	if len(params.Categories) > 0 {
		where += fmt.Sprintf(" AND categories && $%d", argIdx)
		args = append(args, params.Categories)
		argIdx++
	}

	if len(params.Eligibility) > 0 {
		params.Eligibility = sanitizeStringSlice(params.Eligibility)
	}
	if len(params.Eligibility) > 0 {
		where += fmt.Sprintf(" AND eligibility && $%d", argIdx)
		args = append(args, params.Eligibility)
		argIdx++
	}

	// 2. Count Total
	var total int
	countSQL := "SELECT COUNT(*) FROM opportunities " + where
	if err := s.pool.QueryRow(ctx, countSQL, args...).Scan(&total); err != nil {
		return nil, fmt.Errorf("count failed: %w", err)
	}

	// 3. Select Data with Scoring/Sorting
	selectSQL := fmt.Sprintf("SELECT %s FROM opportunities %s", selectCols, where)

	// Sorting
	switch params.SortBy {
	case "deadline":
		selectSQL += " ORDER BY next_deadline_at ASC NULLS LAST, deadline_at ASC NULLS LAST"
	case "amount_desc":
		selectSQL += " ORDER BY amount_max DESC NULLS LAST"
	case "newest":
		selectSQL += " ORDER BY open_date DESC NULLS LAST, created_at DESC"
	default: // "relevance"
		if len(params.QueryEmbedding) > 0 {
			vectorArg := argIdx
			queryArg := argIdx + 1
			args = append(args, pgvector.NewVector(params.QueryEmbedding), params.Query)
			argIdx += 2

			selectSQL += fmt.Sprintf(`
				ORDER BY
					CASE WHEN embedding IS NULL THEN 1 ELSE 0 END ASC,
					COALESCE(1 - (embedding <=> $%d), -1) DESC,
					CASE WHEN NULLIF($%d::text, '') IS NULL THEN 0 ELSE ts_rank(search_vector, plainto_tsquery('english', $%d::text)) END DESC,
					updated_at DESC NULLS LAST,
					created_at DESC
			`, vectorArg, queryArg, queryArg)
		} else if params.Query != "" {
			queryArg := argIdx
			args = append(args, params.Query)
			argIdx++
			selectSQL += fmt.Sprintf(" ORDER BY ts_rank(search_vector, plainto_tsquery('english', $%d::text)) DESC, updated_at DESC NULLS LAST, created_at DESC", queryArg)
		} else {
			selectSQL += " ORDER BY updated_at DESC NULLS LAST, created_at DESC"
		}
	}

	// Pagination
	selectSQL += fmt.Sprintf(" LIMIT $%d OFFSET $%d", argIdx, argIdx+1)
	args = append(args, params.Limit, params.Offset)

	// Execute
	rows, err := s.pool.Query(ctx, selectSQL, args...)
	if err != nil {
		return nil, fmt.Errorf("query failed: %w", err)
	}
	defer rows.Close()

	var opps []models.Opportunity
	for rows.Next() {
		o, err := scanOpportunity(rows.Scan)
		if err != nil {
			return nil, fmt.Errorf("scan failed: %w", err)
		}
		opps = append(opps, o)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration failed: %w", err)
	}

	if opps == nil {
		opps = []models.Opportunity{}
	}

	return &ListResult{
		Opportunities: opps,
		Total:         total,
		Limit:         params.Limit,
		Offset:        params.Offset,
	}, nil
}

func buildOpenTabConstraint() string {
	return " AND normalized_status = 'open' AND is_results_page = false AND (rolling_evidence = true OR next_deadline_at >= NOW() OR close_at >= NOW())"
}

func decodeDeadlineDates(raw []byte) []string {
	var stringsPayload []string
	if err := json.Unmarshal(raw, &stringsPayload); err == nil {
		return sanitizeStringSlice(stringsPayload)
	}

	var evidencePayload []map[string]interface{}
	if err := json.Unmarshal(raw, &evidencePayload); err != nil {
		return nil
	}

	result := make([]string, 0, len(evidencePayload))
	for _, entry := range evidencePayload {
		if value, ok := entry["parsed_date_iso"].(string); ok && strings.TrimSpace(value) != "" {
			result = append(result, strings.TrimSpace(value))
		}
	}

	if len(result) == 0 {
		return nil
	}

	// Preserve deterministic ascending order when values are parseable timestamps.
	sortable := make([]time.Time, 0, len(result))
	for _, item := range result {
		if ts, err := time.Parse(time.RFC3339, item); err == nil {
			sortable = append(sortable, ts.UTC())
		}
	}
	if len(sortable) == len(result) {
		sorted := make([]string, 0, len(sortable))
		for i := 0; i < len(sortable); i++ {
			minIdx := i
			for j := i + 1; j < len(sortable); j++ {
				if sortable[j].Before(sortable[minIdx]) {
					minIdx = j
				}
			}
			sortable[i], sortable[minIdx] = sortable[minIdx], sortable[i]
			sorted = append(sorted, sortable[i].Format(time.RFC3339))
		}
		return sorted
	}

	return sanitizeStringSlice(result)
}

func sanitizeStringSlice(values []string) []string {
	if len(values) == 0 {
		return values
	}

	clean := make([]string, 0, len(values))
	for _, value := range values {
		trimmed := strings.TrimSpace(value)
		if trimmed != "" {
			clean = append(clean, trimmed)
		}
	}

	return clean
}

func (s *Store) GetOpportunity(ctx context.Context, id string) (*models.Opportunity, error) {
	sql := fmt.Sprintf(`
		SELECT %s
		FROM opportunities
		WHERE id = $1
	`, selectCols)
	row := s.pool.QueryRow(ctx, sql, id)

	o, err := scanOpportunity(row.Scan)
	if err != nil {
		return nil, fmt.Errorf("not found: %w", err)
	}

	return &o, nil
}

func (s *Store) GetOpportunityBySourceID(ctx context.Context, sourceDomain, sourceID string) (*models.Opportunity, error) {
	sql := fmt.Sprintf(`
		SELECT %s
		FROM opportunities
		WHERE source_domain = $1 AND source_id = $2
	`, selectCols)
	row := s.pool.QueryRow(ctx, sql, sourceDomain, sourceID)

	o, err := scanOpportunity(row.Scan)
	if err != nil {
		return nil, fmt.Errorf("not found: %w", err)
	}

	return &o, nil
}

func (s *Store) GetSources(ctx context.Context) ([]string, error) {
	rows, err := s.pool.Query(ctx, "SELECT DISTINCT source_domain FROM opportunities ORDER BY source_domain")
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var sources []string
	for rows.Next() {
		var src string
		if err := rows.Scan(&src); err == nil {
			sources = append(sources, src)
		}
	}
	return sources, nil
}

func (s *Store) GetStats(ctx context.Context) (map[string]interface{}, error) {
	stats := make(map[string]interface{})

	var total int
	s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM opportunities").Scan(&total)
	stats["total"] = total

	var sources int
	s.pool.QueryRow(ctx, "SELECT COUNT(DISTINCT source_domain) FROM opportunities").Scan(&sources)
	stats["sources"] = sources

	var rolling int
	s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM opportunities WHERE is_rolling = true").Scan(&rolling)
	stats["rolling"] = rolling

	var withDeadline int
	s.pool.QueryRow(ctx, "SELECT COUNT(*) FROM opportunities WHERE next_deadline_at IS NOT NULL AND next_deadline_at > NOW()").Scan(&withDeadline)
	stats["with_deadline"] = withDeadline

	statusCounts := map[string]int{}
	rows, err := s.pool.Query(ctx, "SELECT normalized_status::text, COUNT(*) FROM opportunities GROUP BY normalized_status")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var status string
			var count int
			if scanErr := rows.Scan(&status, &count); scanErr == nil {
				statusCounts[status] = count
			}
		}
	}
	stats["normalized_status_counts"] = statusCounts

	return stats, nil
}

// Aggregation represents a single facet count.
type Aggregation struct {
	Value string `json:"value"`
	Count int    `json:"count"`
}

// AggregationResult contains all facet counts for the sidebar filters.
type AggregationResult struct {
	Regions     []Aggregation `json:"regions"`
	FunderTypes []Aggregation `json:"funder_types"`
	Agencies    []Aggregation `json:"agencies"`
	Countries   []Aggregation `json:"countries"`
}

// AggregationParams controls which subset of opportunities is used for facet counts.
type AggregationParams struct {
	Status     string // "open", "closed", "all", etc.
	Region     []string
	FunderType []string
	Country    []string
	AgencyName []string
}

func (s *Store) GetAggregations(ctx context.Context, params AggregationParams) (*AggregationResult, error) {
	result := &AggregationResult{}

	// Cross-faceted filtering: each dimension's query EXCLUDES its own filter
	// so the sidebar always shows all options with correct counts.

	// Regions — exclude region filter
	{
		w, a := buildAggregationWhereExcluding(params, "region")
		q := fmt.Sprintf(`SELECT COALESCE(region, 'Unknown'), COUNT(*) FROM opportunities %s GROUP BY region ORDER BY COUNT(*) DESC`, w)
		rows, err := s.pool.Query(ctx, q, a...)
		if err == nil {
			for rows.Next() {
				var ag Aggregation
				if err := rows.Scan(&ag.Value, &ag.Count); err == nil && ag.Value != "" {
					result.Regions = append(result.Regions, ag)
				}
			}
			rows.Close()
		}
	}

	// Funder Types — exclude funder_type filter
	{
		w, a := buildAggregationWhereExcluding(params, "funder_type")
		q := fmt.Sprintf(`SELECT COALESCE(funder_type, 'Unknown'), COUNT(*) FROM opportunities %s GROUP BY funder_type ORDER BY COUNT(*) DESC`, w)
		rows, err := s.pool.Query(ctx, q, a...)
		if err == nil {
			for rows.Next() {
				var ag Aggregation
				if err := rows.Scan(&ag.Value, &ag.Count); err == nil && ag.Value != "" {
					result.FunderTypes = append(result.FunderTypes, ag)
				}
			}
			rows.Close()
		}
	}

	// Agencies — exclude agency_name filter
	{
		w, a := buildAggregationWhereExcluding(params, "agency_name")
		q := fmt.Sprintf(`SELECT COALESCE(agency_name, 'Unknown'), COUNT(*) FROM opportunities %s AND agency_name IS NOT NULL AND agency_name != '' GROUP BY agency_name ORDER BY COUNT(*) DESC`, w)
		rows, err := s.pool.Query(ctx, q, a...)
		if err == nil {
			for rows.Next() {
				var ag Aggregation
				if err := rows.Scan(&ag.Value, &ag.Count); err == nil {
					result.Agencies = append(result.Agencies, ag)
				}
			}
			rows.Close()
		}
	}

	// Countries — exclude country filter
	{
		w, a := buildAggregationWhereExcluding(params, "country")
		q := fmt.Sprintf(`SELECT COALESCE(country, 'Unknown'), COUNT(*) FROM opportunities %s AND country IS NOT NULL AND country != '' GROUP BY country ORDER BY COUNT(*) DESC LIMIT 50`, w)
		rows, err := s.pool.Query(ctx, q, a...)
		if err == nil {
			for rows.Next() {
				var ag Aggregation
				if err := rows.Scan(&ag.Value, &ag.Count); err == nil {
					result.Countries = append(result.Countries, ag)
				}
			}
			rows.Close()
		}
	}

	return result, nil
}

// buildAggregationWhereExcluding constructs a WHERE clause that mirrors the status
// filtering used by ListOpportunities. The `exclude` parameter names the dimension
// to omit, implementing cross-faceted filtering so each sidebar section always
// shows all available options (not just the currently selected one).
func buildAggregationWhereExcluding(params AggregationParams, exclude string) (string, []interface{}) {
	where := "WHERE 1=1"
	var args []interface{}
	argIdx := 1

	// Status is never excluded — it applies to all dimensions.
	status := params.Status
	if status == "" || status == "active" {
		status = "open"
	}

	if status == "open" {
		where += " AND normalized_status = 'open' AND is_results_page = false AND (rolling_evidence = true OR next_deadline_at >= NOW() OR close_at >= NOW())"
	} else if status == "closed" {
		where += " AND normalized_status::text IN ('closed','archived')"
	} else if status != "all" {
		if status == "posted" {
			status = "open"
		}
		where += fmt.Sprintf(" AND normalized_status::text = $%d", argIdx)
		args = append(args, status)
		argIdx++
	}

	if len(params.Region) > 0 && exclude != "region" {
		where += fmt.Sprintf(" AND region = ANY($%d)", argIdx)
		args = append(args, params.Region)
		argIdx++
	}
	if len(params.FunderType) > 0 && exclude != "funder_type" {
		where += fmt.Sprintf(" AND funder_type = ANY($%d)", argIdx)
		args = append(args, params.FunderType)
		argIdx++
	}
	if len(params.Country) > 0 && exclude != "country" {
		where += fmt.Sprintf(" AND country = ANY($%d)", argIdx)
		args = append(args, params.Country)
		argIdx++
	}
	if len(params.AgencyName) > 0 && exclude != "agency_name" {
		where += fmt.Sprintf(" AND agency_name = ANY($%d)", argIdx)
		args = append(args, params.AgencyName)
		argIdx++
	}

	return where, args
}
