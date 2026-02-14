package models

import (
	"time"

	"github.com/google/uuid"
)

type Opportunity struct {
	ID                uuid.UUID              `json:"id"`
	Title             string                 `json:"title"`
	Summary           string                 `json:"summary"`
	ExternalURL       string                 `json:"external_url"`
	SourceDomain      string                 `json:"source_domain"`
	SourceID          string                 `json:"source_id"`
	OpportunityNumber string                 `json:"opportunity_number"`
	AgencyName        string                 `json:"agency_name"`
	AgencyCode        string                 `json:"agency_code"`
	FunderType        string                 `json:"funder_type"`
	AmountMin         float64                `json:"amount_min"`
	AmountMax         float64                `json:"amount_max"`
	Currency          string                 `json:"currency"`
	DeadlineAt        *time.Time             `json:"deadline_at"`
	NextDeadlineAt    *time.Time             `json:"next_deadline_at"`
	OpenDate          *time.Time             `json:"open_date"`
	OpenAt            *time.Time             `json:"open_at"`
	CloseAt           *time.Time             `json:"close_at"`
	ExpirationAt      *time.Time             `json:"expiration_at"`
	IsRolling         bool                   `json:"is_rolling"`
	DocType           string                 `json:"doc_type"`
	CfdaList          []string               `json:"cfda_list"`
	OppStatus         string                 `json:"opp_status"`
	SourceStatusRaw   string                 `json:"source_status_raw"`
	NormalizedStatus  string                 `json:"normalized_status"`
	StatusReason      string                 `json:"status_reason"`
	StatusConfidence  float64                `json:"status_confidence"`
	Deadlines         []string               `json:"deadlines"`
	IsResultsPage     bool                   `json:"is_results_page"`
	RollingEvidence   bool                   `json:"rolling_evidence"`
	SourceEvidenceJSON map[string]interface{} `json:"source_evidence_json"`
	Region            string                 `json:"region"`
	Country           string                 `json:"country"`
	Categories        []string               `json:"categories"`
	Eligibility       []string               `json:"eligibility"`
	Description       string                 `json:"description"`    // Full HTML description
	CloseDateRaw      string                 `json:"close_date_raw"` // Original text for deadline
	CreatedAt         time.Time              `json:"created_at"`
	UpdatedAt         time.Time              `json:"updated_at"`
	SourceRunID       *string                `json:"source_run_id"`
	CanonicalURL      string                 `json:"canonical_url"`
	RawURL            string                 `json:"raw_url"`
	ContentType       string                 `json:"content_type"`
	DataQualityScore  map[string]interface{} `json:"data_quality_score"`
}
