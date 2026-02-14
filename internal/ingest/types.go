package ingest

import (
	"context"
	"io"
	"time"
)

type DeadlineEvidence struct {
	Source        string  `json:"source"`
	URL           string  `json:"url,omitempty"`
	Snippet       string  `json:"snippet,omitempty"`
	ParsedDateISO string  `json:"parsed_date_iso"`
	Label         string  `json:"label,omitempty"`
	Confidence    float64 `json:"confidence"`
}

// Opportunity represents the structured data extracted from a source.
type Opportunity struct {
	Title             string
	Summary           string
	Description       string
	ExternalURL       string
	SourceDomain      string
	SourceID          string // External source ID (e.g., Grants.gov ID)
	OpportunityNumber string // Opportunity number (e.g., "RFA-NS-27-001")
	AgencyName        string // Full agency name
	AgencyCode        string // Agency code (e.g., "HHS-NIH")
	FunderType        string // Government, Foundation, Corporate, Multilateral
	DeadlineStr       string // Raw string, needs parsing
	DeadlineAt        *time.Time
	OpenDate          *time.Time
	CloseDateRaw      string // Original close date string
	SourceStatusRaw   string
	NormalizedStatus  string
	StatusReason      string
	StatusConfidence  float64
	NextDeadlineAt    *time.Time
	ExpirationAt      *time.Time
	CloseAt           *time.Time
	OpenAt            *time.Time
	Deadlines         []string
	DeadlineEvidence  []DeadlineEvidence
	IsResultsPage     bool
	RollingEvidence   bool
	SourceEvidenceJSON map[string]interface{}
	AmountMin         float64
	AmountMax         float64
	Currency          string
	IsRolling         bool
	DocType           string   // synopsis, forecasted, etc.
	CfdaList          []string // CFDA/ALN numbers
	OppStatus         string   // posted, closed, archived
	Region            string   // North America, Europe, Asia-Pacific, etc.
	Country           string   // USA, UK, etc.
	Category          string
	Type              string // grant, fellowship, prize, award
	Eligibility       []string
	Categories        []string
	RawHTML           string
	Embedding         []float32
	// New Production Fields
	SourceRunID      string
	CanonicalURL     string
	RawURL           string
	ContentType      string
	DataQualityScore map[string]interface{}
}

// RawOpportunity represents the untrusted, unnormalized data extracted from a source.
type RawOpportunity struct {
	Title        string
	Description  string
	ExternalURL  string
	SourceID     string // Provided by source if available, or generated
	SourceDomain string
	RawDeadline  string
	RawAmount    string
	RawCurrency  string
	RawTags      []string
	RawStatus    string
	OpenISO      string
	CloseISO     string
	ExpirationISO string
	DeadlineCandidates []string
	IsResultsPage bool
	RollingEvidence bool
	DeadlineEvidence []DeadlineEvidence
	SourceEvidenceJSON map[string]interface{}
	Extra        map[string]string
}

// FetchedDocument represents the raw result of a fetch operation.
type FetchedDocument struct {
	URL         string
	StatusCode  int
	ContentType string
	Body        io.ReadCloser
	FetchedAt   time.Time
	Headers     map[string][]string
}

// Fetcher retrieves raw content from a URL.
type Fetcher interface {
	Fetch(ctx context.Context, url string) (*FetchedDocument, error)
}

// Parser extracts structured opportunities from raw content.
type Parser interface {
	Parse(ctx context.Context, r io.Reader) ([]Opportunity, error)
}

// Source defines configuration for a data source.
type Source struct {
	Name     string
	URL      string
	Strategy Strategy
	Active   bool
}

// Strategy defines how a specific source should be processed.
type Strategy string

const (
	StrategyAPI  Strategy = "API"  // Structured API (Grants.gov, EU Portal)
	StrategyRSS  Strategy = "RSS"  // RSS/Atom feeds
	StrategyHTML Strategy = "HTML" // HTML pages (needs LLM extraction)
	StrategyLLM  Strategy = "LLM"  // Direct LLM extraction
)
