package ingest

import (
	"testing"
	"time"
)

func TestComputeStatusDecision_ResultsPageClosed(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)
	opp := Opportunity{
		Title:       "ProInnóvate - Resultados finales Startup Perú",
		Summary:     "Publicación de ganadores",
		ExternalURL: "https://proinnovate.gob.pe/resultados-finales",
	}

	decision := ComputeStatusDecision(opp, now)
	if decision.NormalizedStatus != "closed" {
		t.Fatalf("expected closed, got %s", decision.NormalizedStatus)
	}
	if !decision.IsResultsPage {
		t.Fatal("expected is_results_page=true")
	}
	if decision.StatusReason != "results_page" {
		t.Fatalf("expected reason results_page, got %s", decision.StatusReason)
	}
}

func TestComputeStatusDecision_PastDeadlineClosed(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)
	past := now.Add(-48 * time.Hour)

	decision := ComputeStatusDecision(Opportunity{DeadlineAt: &past}, now)
	if decision.NormalizedStatus != "closed" {
		t.Fatalf("expected closed, got %s", decision.NormalizedStatus)
	}
}

func TestComputeStatusDecision_RollingOpen(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)

	decision := ComputeStatusDecision(Opportunity{IsRolling: true, SourceStatusRaw: "rolling call"}, now)
	if decision.NormalizedStatus != "open" {
		t.Fatalf("expected open, got %s", decision.NormalizedStatus)
	}
}

func TestComputeStatusDecision_RollingWithoutEvidenceNeedsReview(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)

	decision := ComputeStatusDecision(Opportunity{IsRolling: true, SourceStatusRaw: "open"}, now)
	if decision.NormalizedStatus != "needs_review" {
		t.Fatalf("expected needs_review, got %s", decision.NormalizedStatus)
	}
	if decision.StatusReason != "rolling_without_evidence" {
		t.Fatalf("expected rolling_without_evidence, got %s", decision.StatusReason)
	}
}

func TestComputeStatusDecision_SourceOpenWithoutDatesNeedsReview(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)

	decision := ComputeStatusDecision(Opportunity{SourceStatusRaw: "open"}, now)
	if decision.NormalizedStatus != "needs_review" {
		t.Fatalf("expected needs_review, got %s", decision.NormalizedStatus)
	}
}

func TestComputeStatusDecision_InconsistentNeedsReview(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)
	future := now.Add(72 * time.Hour)
	decision := ComputeStatusDecision(Opportunity{
		SourceStatusRaw: "closed",
		DeadlineAt:      &future,
	}, now)

	if decision.NormalizedStatus != "needs_review" {
		t.Fatalf("expected needs_review, got %s", decision.NormalizedStatus)
	}
	if decision.StatusReason != "inconsistent_dates" {
		t.Fatalf("expected reason inconsistent_dates, got %s", decision.StatusReason)
	}
}

func TestComputeStatusDecision_MultipleDeadlinesUsesNextFutureMin(t *testing.T) {
	now := time.Date(2026, 2, 12, 12, 0, 0, 0, time.UTC)
	decision := ComputeStatusDecision(Opportunity{
		Deadlines: []string{
			"2026-01-01",
			"2026-02-20",
			"2026-03-10",
		},
	}, now)

	if decision.NormalizedStatus != "open" {
		t.Fatalf("expected open, got %s", decision.NormalizedStatus)
	}
	if decision.NextDeadlineAt == nil {
		t.Fatal("expected next_deadline_at to be set")
	}
	expected := time.Date(2026, 2, 20, 0, 0, 0, 0, time.UTC)
	if !decision.NextDeadlineAt.Equal(expected) {
		t.Fatalf("expected %s, got %s", expected, decision.NextDeadlineAt.UTC())
	}
}
