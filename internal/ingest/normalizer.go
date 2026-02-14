package ingest

import (
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

// TruncateText cuts a string to max length, appending ellipsis if truncated.
func TruncateText(text string, maxLen int) string {
	if len(text) <= maxLen {
		return text
	}
	if maxLen > 3 {
		return text[:maxLen-3] + "..."
	}
	return text[:maxLen]
}

// HTMLToText converts HTML to plain text, collapsing whitespace.
func HTMLToText(html string) string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(html))
	if err != nil {
		return html // Fallback to original if parsing fails
	}
	text := doc.Text()
	return cleanText(text)
}

// FromRaw converts a RawOpportunity into a canonical Opportunity.
func FromRaw(raw RawOpportunity) Opportunity {
	opp := Opportunity{
		Title:        raw.Title,
		ExternalURL:  raw.ExternalURL,
		SourceDomain: raw.SourceDomain,
		SourceID:     raw.SourceID,
		Description:  raw.Description,
		Summary:      raw.Description, // Initial summary is full description
		CloseDateRaw: raw.RawDeadline,
		OppStatus:    "posted", // Default to posted (active) unless evidence says otherwise
		NormalizedStatus: "needs_review",
		CanonicalURL: CanonicalizeURL(raw.ExternalURL),
		RawURL:       raw.ExternalURL,
		ContentType:  "html",
		Categories:   raw.RawTags,
		SourceStatusRaw: raw.RawStatus,
		Deadlines: raw.DeadlineCandidates,
		DeadlineEvidence: raw.DeadlineEvidence,
		IsResultsPage: raw.IsResultsPage,
		RollingEvidence: raw.RollingEvidence,
		SourceEvidenceJSON: raw.SourceEvidenceJSON,
		// CreatedAt/UpdatedAt handled by DB or Pipeline defaults
	}

	// 1. Parse Date
	locales := []string{"en"}
	if locs, ok := raw.Extra["date_locales"]; ok && locs != "" {
		locales = strings.Split(locs, ",")
	}
	if raw.RawDeadline != "" {
		if dt, err := parseDateRobust(raw.RawDeadline, locales); err == nil {
			opp.DeadlineAt = &dt
		}
	}

	// 2. Parse Amount
	if raw.RawAmount != "" {
		defaultCurrency := "USD"
		if raw.RawCurrency != "" {
			defaultCurrency = raw.RawCurrency
		}
		// parseAmountRobust is in amount_parser.go (same package)
		min, max, currency := parseAmountRobust(raw.RawAmount, defaultCurrency)
		if min > 0 || max > 0 {
			opp.AmountMin = min
			opp.AmountMax = max
			if currency != "" {
				opp.Currency = currency
			}
		}
	}

	// 3. Extra Fields Handling
	if val, ok := raw.Extra["is_rolling"]; ok && val == "true" {
		opp.IsRolling = true
		opp.RollingEvidence = true
	}
	if val, ok := raw.Extra["opp_status"]; ok && val != "" {
		opp.OppStatus = val
		if opp.SourceStatusRaw == "" {
			opp.SourceStatusRaw = val
		}
	}
	if val, ok := raw.Extra["source_status_raw"]; ok && val != "" {
		opp.SourceStatusRaw = val
	}
	if val, ok := raw.Extra["is_results_page"]; ok && strings.EqualFold(val, "true") {
		opp.IsResultsPage = true
	}
	if val, ok := raw.Extra["eligibility"]; ok && val != "" {
		opp.Eligibility = mergeUniqueFold(opp.Eligibility, splitAndCleanList(val))
	}

	if raw.OpenISO != "" {
		if dt, ok := parseDeadlineCandidate(raw.OpenISO); ok {
			opp.OpenAt = &dt
		}
	}
	if raw.CloseISO != "" {
		if dt, ok := parseDeadlineCandidate(raw.CloseISO); ok {
			opp.CloseAt = &dt
		}
	}
	if raw.ExpirationISO != "" {
		if dt, ok := parseDeadlineCandidate(raw.ExpirationISO); ok {
			opp.ExpirationAt = &dt
		}
	}

	// 3. Normalize Text & Regions
	NormalizeOpportunity(&opp)

	// 4. Apply Business Rules for Status
	UpdateStatus(&opp)

	return opp
}

// NormalizeOpportunity cleans and standardizes opportunity data in place.
func NormalizeOpportunity(opp *Opportunity) {
	opp.Title = cleanText(opp.Title)
	opp.Summary = cleanText(opp.Summary)
	opp.Region = normalizeRegion(opp.Region)
	opp.Country = normalizeCountry(opp.Country)
	opp.FunderType = normalizeFunderType(opp.FunderType)
	opp.AgencyName = cleanText(opp.AgencyName)

	// Removed: inferStatusFromTitle heuristic (removed per user request to avoid false positives)
}

// UpdateStatus applies the single source of truth for opportunity status.
// Rules:
// - If explicitly 'closed' or 'archived', stay closed.
// - If IsRolling -> posted.
// - If Deadline parsing failed -> posted (default, do not force unknown).
// - If Deadline < Now -> closed.
// - If Deadline >= Now -> posted.
func UpdateStatus(opp *Opportunity) {
	decision := ComputeStatusDecision(*opp, time.Now().UTC())
	opp.NormalizedStatus = decision.NormalizedStatus
	opp.StatusReason = decision.StatusReason
	opp.StatusConfidence = decision.StatusConfidence
	opp.NextDeadlineAt = decision.NextDeadlineAt
	opp.IsResultsPage = decision.IsResultsPage

	if decision.NormalizedStatus == "closed" || decision.NormalizedStatus == "archived" {
		opp.OppStatus = decision.NormalizedStatus
		return
	}
	if opp.OppStatus == "" || opp.OppStatus == "unknown" {
		opp.OppStatus = "posted"
	}
}
