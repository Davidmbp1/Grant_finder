package ingest

import (
	"strings"
	"testing"
	"time"
)

func TestParseDateCandidatesFromText_Multilingual(t *testing.T) {
	text := `Submission closes on 17 June 2025 1 p.m. and final date 30/06/2025.
	También: fecha de cierre 21 de julio del 2025.`

	candidates := parseDateCandidatesFromText(text)
	if len(candidates) < 3 {
		t.Fatalf("expected at least 3 candidates, got %d", len(candidates))
	}

	joined := strings.Join(candidates, "|")
	if !strings.Contains(joined, "2025-06-17") {
		t.Fatalf("expected parsed EN date with time token, got %v", candidates)
	}
	if !strings.Contains(joined, "2025-06-30") {
		t.Fatalf("expected parsed dd/mm/yyyy date, got %v", candidates)
	}
	if !strings.Contains(joined, "2025-07-21") {
		t.Fatalf("expected parsed ES month name date, got %v", candidates)
	}
}

func TestNormalizeDateOnlyBySource_GobPeUsesEndOfDayUTC(t *testing.T) {
	parsed := time.Date(2026, 2, 18, 0, 0, 0, 0, time.UTC)
	normalized := normalizeDateOnlyBySource(parsed, "https://www.gob.pe/institucion/proinnovate/campanas/x")

	if normalized.Hour() < 4 || normalized.Hour() > 6 {
		t.Fatalf("expected America/Lima end-of-day normalized near 04-06 UTC, got %s", normalized.Format(time.RFC3339))
	}
	if normalized.Day() != 19 && normalized.Day() != 18 {
		t.Fatalf("expected normalized date around close day boundary, got %s", normalized.Format(time.RFC3339))
	}
}
