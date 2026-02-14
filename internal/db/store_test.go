package db

import (
	"strings"
	"testing"
)

func TestBuildOpenTabConstraint_IsStrict(t *testing.T) {
	clause := buildOpenTabConstraint()

	mustContain := []string{
		"normalized_status = 'open'",
		"is_results_page = false",
		"rolling_evidence = true OR next_deadline_at >= NOW() OR close_at >= NOW()",
	}

	for _, token := range mustContain {
		if !strings.Contains(clause, token) {
			t.Fatalf("open clause missing token %q: %s", token, clause)
		}
	}

	if strings.Contains(clause, "next_deadline_at IS NULL") {
		t.Fatalf("open clause must not allow null deadlines by default: %s", clause)
	}
}
