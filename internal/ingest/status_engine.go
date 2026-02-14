package ingest

import (
	"strings"
	"time"
)

type StatusDecision struct {
	NormalizedStatus string
	StatusReason     string
	StatusConfidence float64
	NextDeadlineAt   *time.Time
	IsResultsPage    bool
}

// resultsKeywords are phrases that indicate a page is displaying results/winners
// rather than an active call for proposals. These are matched against title,
// summary, description and source_status_raw — but NOT against URLs.
var resultsKeywords = []string{
	"resultados finales",
	"ganadores",
	"final results",
	"winners announced",
	"awards announced",
	"awarded to",
	"awardees selected",
	"convocatoria cerrada",
	"cierre de postulaciones",
	"results published",
	"results available",
	"ranking final",
}

func ComputeStatusDecision(opp Opportunity, now time.Time) StatusDecision {
	now = now.UTC()

	nextDeadline := pickNextDeadline(opp, now)
	isResults := detectResultsPage(opp)
	hasRollingEvidence := detectRollingEvidence(opp)
	effectiveRolling := hasRollingEvidence

	if isResults || opp.IsResultsPage {
		return StatusDecision{
			NormalizedStatus: "closed",
			StatusReason:     "results_page",
			StatusConfidence: 0.99,
			NextDeadlineAt:   nextDeadline,
			IsResultsPage:    true,
		}
	}

	mappedSource := mapSourceStatusRaw(opp.SourceStatusRaw)
	if mappedSource == "" {
		mappedSource = mapSourceStatusRaw(opp.OppStatus)
	}

	if mappedSource == "archived" {
		return StatusDecision{NormalizedStatus: "archived", StatusReason: "source_archived", StatusConfidence: 0.95, NextDeadlineAt: nextDeadline}
	}

	if mappedSource == "closed" {
		if effectiveRolling || (nextDeadline != nil && nextDeadline.After(now)) || (opp.CloseAt != nil && opp.CloseAt.After(now)) {
			return StatusDecision{NormalizedStatus: "needs_review", StatusReason: "inconsistent_dates", StatusConfidence: 0.35, NextDeadlineAt: nextDeadline}
		}
		return StatusDecision{NormalizedStatus: "closed", StatusReason: "source_closed", StatusConfidence: 0.92, NextDeadlineAt: nextDeadline}
	}

	if opp.OpenAt != nil && opp.OpenAt.After(now) {
		return StatusDecision{NormalizedStatus: "upcoming", StatusReason: "open_date_in_future", StatusConfidence: 0.9, NextDeadlineAt: nextDeadline}
	}

	if opp.IsRolling && !hasRollingEvidence {
		return StatusDecision{NormalizedStatus: "needs_review", StatusReason: "rolling_without_evidence", StatusConfidence: 0.2, NextDeadlineAt: nextDeadline}
	}

	if effectiveRolling {
		return StatusDecision{NormalizedStatus: "open", StatusReason: "rolling_open", StatusConfidence: 0.96, NextDeadlineAt: nextDeadline}
	}

	if nextDeadline != nil && nextDeadline.After(now) {
		return StatusDecision{NormalizedStatus: "open", StatusReason: "future_deadline", StatusConfidence: 0.93, NextDeadlineAt: nextDeadline}
	}

	if opp.CloseAt != nil && opp.CloseAt.After(now) {
		return StatusDecision{NormalizedStatus: "open", StatusReason: "future_close_date", StatusConfidence: 0.9, NextDeadlineAt: nextDeadline}
	}

	if opp.CloseAt != nil && !opp.CloseAt.After(now) {
		return StatusDecision{NormalizedStatus: "closed", StatusReason: "close_date_passed", StatusConfidence: 0.94, NextDeadlineAt: nextDeadline}
	}

	if opp.ExpirationAt != nil && !opp.ExpirationAt.After(now) {
		return StatusDecision{NormalizedStatus: "closed", StatusReason: "expiration_passed", StatusConfidence: 0.9, NextDeadlineAt: nextDeadline}
	}

	if opp.DeadlineAt != nil && !opp.DeadlineAt.After(now) {
		return StatusDecision{NormalizedStatus: "closed", StatusReason: "deadline_passed", StatusConfidence: 0.95, NextDeadlineAt: nextDeadline}
	}

	if hasAnyDeadlineEvidence(opp) && !hasFutureDeadlineEvidence(opp, now) {
		return StatusDecision{NormalizedStatus: "closed", StatusReason: "deadline_passed", StatusConfidence: 0.95, NextDeadlineAt: nextDeadline}
	}

	if mappedSource == "upcoming" {
		return StatusDecision{NormalizedStatus: "upcoming", StatusReason: "source_upcoming", StatusConfidence: 0.75, NextDeadlineAt: nextDeadline}
	}

	if mappedSource == "open" {
		return StatusDecision{NormalizedStatus: "needs_review", StatusReason: "source_open_without_time_evidence", StatusConfidence: 0.3, NextDeadlineAt: nextDeadline}
	}

	if nextDeadline == nil && !effectiveRolling && (opp.CloseAt == nil || !opp.CloseAt.After(now)) {
		return StatusDecision{NormalizedStatus: "needs_review", StatusReason: "missing_deadline", StatusConfidence: 0.25, NextDeadlineAt: nil}
	}

	return StatusDecision{NormalizedStatus: "needs_review", StatusReason: "inconsistent_dates", StatusConfidence: 0.4, NextDeadlineAt: nextDeadline}
}

func detectRollingEvidence(opp Opportunity) bool {
	if opp.RollingEvidence {
		return true
	}

	joined := strings.ToLower(strings.Join([]string{
		opp.SourceStatusRaw,
		opp.OppStatus,
		opp.Title,
		opp.Summary,
		HTMLToText(opp.Description),
	}, " \n "))

	rollingHints := []string{
		"rolling", "rolling basis", "open continuously", "ongoing call",
		"open until filled", "no deadline", "ventanilla abierta",
		"convocatoria permanente", "sin fecha límite", "abierta permanentemente",
	}
	for _, hint := range rollingHints {
		if strings.Contains(joined, hint) {
			return true
		}
	}

	if evidenceRolling, ok := opp.SourceEvidenceJSON["rolling_evidence"].(bool); ok && evidenceRolling {
		return true
	}

	return false
}

func pickNextDeadline(opp Opportunity, now time.Time) *time.Time {
	var best *time.Time
	var labeledCloseBest *time.Time

	candidates := make([]time.Time, 0, len(opp.Deadlines)+2)
	for _, raw := range opp.Deadlines {
		if t, ok := parseDeadlineCandidate(raw); ok {
			candidates = append(candidates, t.UTC())
		}
	}
	for _, ev := range opp.DeadlineEvidence {
		if t, ok := parseDeadlineCandidate(ev.ParsedDateISO); ok {
			candidates = append(candidates, t.UTC())
			label := strings.ToLower(ev.Label + " " + ev.Snippet)
			isStartLike := strings.Contains(label, "inicio") || strings.Contains(label, "apertura") || strings.Contains(label, "start") || strings.Contains(label, "opening")
			isCloseLike := strings.Contains(label, "cierre") || strings.Contains(label, "deadline") || strings.Contains(label, "closes") || strings.Contains(label, "submission") || (strings.Contains(label, "postul") && !isStartLike)
			if isCloseLike && !isStartLike && t.After(now) {
				tu := t.UTC()
				if labeledCloseBest == nil || tu.Before(*labeledCloseBest) {
					labeledCloseBest = &tu
				}
			}
		}
	}

	if labeledCloseBest != nil {
		return labeledCloseBest
	}

	if opp.NextDeadlineAt != nil {
		candidates = append(candidates, opp.NextDeadlineAt.UTC())
	}
	if opp.DeadlineAt != nil {
		candidates = append(candidates, opp.DeadlineAt.UTC())
	}

	for _, candidate := range candidates {
		if !candidate.After(now) {
			continue
		}
		if best == nil || candidate.Before(*best) {
			c := candidate
			best = &c
		}
	}

	if best != nil {
		return best
	}

	if opp.NextDeadlineAt != nil {
		c := opp.NextDeadlineAt.UTC()
		return &c
	}

	return nil
}

func hasAnyDeadlineEvidence(opp Opportunity) bool {
	if len(opp.DeadlineEvidence) > 0 || len(opp.Deadlines) > 0 {
		return true
	}
	return opp.DeadlineAt != nil || opp.NextDeadlineAt != nil
}

func hasFutureDeadlineEvidence(opp Opportunity, now time.Time) bool {
	if next := pickNextDeadline(opp, now); next != nil && next.After(now) {
		return true
	}
	if opp.CloseAt != nil && opp.CloseAt.After(now) {
		return true
	}
	return false
}

func detectResultsPage(opp Opportunity) bool {
	// IMPORTANT: Do NOT include ExternalURL in keyword matching.
	// URLs like grants.gov/search-results-detail/... contain "results" but
	// are NOT results pages — they are detail pages for active grants.
	text := strings.ToLower(strings.Join([]string{
		opp.Title,
		opp.Summary,
		HTMLToText(opp.Description),
		opp.SourceStatusRaw,
	}, " \n "))

	// If the source explicitly says it's posted/active/open, it's not a results page
	srcLower := strings.ToLower(opp.OppStatus)
	if srcLower == "posted" || srcLower == "active" || srcLower == "open" || srcLower == "forecasted" {
		return false
	}

	for _, keyword := range resultsKeywords {
		if strings.Contains(text, keyword) {
			return true
		}
	}

	// ProInnovate-specific check
	if strings.Contains(text, "proinnovate") && (strings.Contains(text, "resultados finales") || strings.Contains(text, "ganadores")) {
		return true
	}

	return false
}

func mapSourceStatusRaw(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	if raw == "" {
		return ""
	}

	closedHints := []string{"closed", "cerrad", "finaliz", "cancel", "funded", "expired", "no longer accepting"}
	for _, hint := range closedHints {
		if strings.Contains(raw, hint) {
			return "closed"
		}
	}

	archivedHints := []string{"archived", "historic", "results", "winners", "awardees", "ganadores", "resultados"}
	for _, hint := range archivedHints {
		if strings.Contains(raw, hint) {
			return "archived"
		}
	}

	upcomingHints := []string{"forthcoming", "upcoming", "coming soon", "próxim", "anticipated"}
	for _, hint := range upcomingHints {
		if strings.Contains(raw, hint) {
			return "upcoming"
		}
	}

	openHints := []string{"open", "posted", "active", "abierta", "vigente", "rolling"}
	for _, hint := range openHints {
		if strings.Contains(raw, hint) {
			return "open"
		}
	}

	return ""
}

func parseDeadlineCandidate(raw string) (time.Time, bool) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}, false
	}

	formats := []string{
		time.RFC3339,
		"2006-01-02",
		"2006-01-02 15:04:05",
		"2006-01-02T15:04:05",
	}

	for _, format := range formats {
		if t, err := time.Parse(format, raw); err == nil {
			return t, true
		}
	}

	return time.Time{}, false
}
