package ingest

import (
	"strings"
	"testing"
	"time"
)

func TestBuildStructuredExtractionText_IncludesTableRows(t *testing.T) {
	html := `
	<html><body>
	<table>
	<tr><th>Actividad</th><th>Fecha</th></tr>
	<tr><td>Cierre de postulaciones</td><td>18 de febrero del 2026</td></tr>
	</table>
	</body></html>`

	out := strings.ToLower(buildStructuredExtractionText(html))
	if !strings.Contains(out, "cierre de postulaciones") {
		t.Fatalf("expected structured text to contain close label, got: %s", out)
	}
	if !strings.Contains(out, "18 de febrero del 2026") {
		t.Fatalf("expected structured text to keep table date, got: %s", out)
	}
}

func TestPickNextDeadline_PrioritizesCloseLabeledFutureDate(t *testing.T) {
	now := time.Date(2026, 2, 1, 0, 0, 0, 0, time.UTC)
	opp := Opportunity{
		DeadlineEvidence: []DeadlineEvidence{
			{ParsedDateISO: "2026-02-10T23:59:59Z", Label: "inicio de postulaciones", Snippet: "Inicio de postulaciones"},
			{ParsedDateISO: "2026-02-20T23:59:59Z", Label: "cierre de postulaciones", Snippet: "Cierre de postulaciones"},
			{ParsedDateISO: "2026-03-01T23:59:59Z", Label: "deadline", Snippet: "Submission deadline"},
		},
	}

	next := pickNextDeadline(opp, now)
	if next == nil {
		t.Fatal("expected next deadline")
	}
	expected := time.Date(2026, 2, 20, 23, 59, 59, 0, time.UTC)
	if !next.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, next.UTC())
	}
}
