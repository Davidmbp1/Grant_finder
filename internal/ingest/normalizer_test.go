package ingest

import (
	"testing"
	"time"
)

func TestUpdateStatus(t *testing.T) {
	now := time.Now()
	past := now.Add(-24 * time.Hour)
	future := now.Add(24 * time.Hour)

	tests := []struct {
		name           string
		opp            Opportunity
		expectedStatus string
	}{
		{
			name: "Closed status remains closed",
			opp: Opportunity{
				OppStatus:  "closed",
				DeadlineAt: &future,
			},
			expectedStatus: "closed",
		},
		{
			name: "Archived status remains archived",
			opp: Opportunity{
				OppStatus:  "archived",
				DeadlineAt: &future,
			},
			expectedStatus: "archived",
		},
		{
			name: "Past deadline becomes closed",
			opp: Opportunity{
				OppStatus:  "posted",
				DeadlineAt: &past,
			},
			expectedStatus: "closed",
		},
		{
			name: "Future deadline stays posted",
			opp: Opportunity{
				OppStatus:  "unknown",
				DeadlineAt: &future,
			},
			expectedStatus: "posted",
		},
		{
			name: "Rolling opportunity is posted",
			opp: Opportunity{
				OppStatus:  "unknown",
				IsRolling:  true,
				DeadlineAt: &past,
			},
			expectedStatus: "posted",
		},
		{
			name: "No deadline defaults to posted",
			opp: Opportunity{
				OppStatus: "unknown",
			},
			expectedStatus: "posted",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			opp := tt.opp
			UpdateStatus(&opp)

			if opp.OppStatus != tt.expectedStatus {
				t.Errorf("expected status %s, got %s", tt.expectedStatus, opp.OppStatus)
			}
		})
	}
}
