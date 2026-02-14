package ingest

import (
	"embed"
	"os"

	"gopkg.in/yaml.v3"
)

//go:embed config/sources.yaml
var sourcesYAML embed.FS

// Registry holds the configuration for all data sources.
type Registry struct {
	Sources []SourceConfig `yaml:"sources"`
}

// FetchConfig defines HTTP fetching configuration for a source.
type FetchConfig struct {
	TimeoutSeconds int     `yaml:"timeout_seconds,omitempty"` // Default: 30
	MaxRetries     int     `yaml:"max_retries,omitempty"`     // Default: 3
	RateLimitRPS   float64 `yaml:"rate_limit_rps,omitempty"`  // Requests per second, default: 1.0
	ProxyURL       string  `yaml:"proxy_url,omitempty"`
	AcceptLanguage string  `yaml:"accept_language,omitempty"` // e.g., "es-PE,es;q=0.9,en;q=0.8"
}

// SourceConfig defines a single data source for ingestion.
type SourceConfig struct {
	ID          string   `yaml:"id"`
	Name        string   `yaml:"name"`
	Kind        string   `yaml:"kind"` // "opportunity", "news"
	Region      string   `yaml:"region"`
	Country     string   `yaml:"country"`
	Strategy    string   `yaml:"strategy"` // "api_grants_gov", "api_eu_ft", "html_generic"
	BaseURL     string   `yaml:"base_url,omitempty"`
	APIKey      string   `yaml:"api_key,omitempty"`
	Seeds       []string `yaml:"seed_urls,omitempty"`
	Schedule    string   `yaml:"schedule,omitempty"`
	Description string   `yaml:"description,omitempty"`

	// HTTP fetching configuration
	Fetch FetchConfig `yaml:"fetch,omitempty"`

	// For generic HTML strategy
	Selectors  SelectorConfig   `yaml:"selectors,omitempty"`
	Pagination PaginationConfig `yaml:"pagination,omitempty"`
	MaxPages   int              `yaml:"max_pages,omitempty"`
	Detail     DetailConfig     `yaml:"detail,omitempty"`
}

type PaginationConfig struct {
	Next string `yaml:"next,omitempty"` // CSS selector for the next page link
}

type SelectorConfig struct {
	Container string `yaml:"container,omitempty"` // CSS selector for the list item wrapper
	Link      string `yaml:"link,omitempty"`
	LinkAttr  string `yaml:"link_attr,omitempty"` // Attribute to extract link from (default: href)
	Title     string `yaml:"title,omitempty"`
	Date      string `yaml:"date,omitempty"` // Date in listing
	Content   string `yaml:"content,omitempty"`
}

// DetailParseConfig defines parsing configuration for detail enrichment.
type DetailParseConfig struct {
	DateLocales     []string `yaml:"date_locales,omitempty"`     // ["en", "es", "pt"]
	CurrencyDefault string   `yaml:"currency_default,omitempty"` // "USD", "EUR", "GBP"
	DateFormats     []string `yaml:"date_formats,omitempty"`     // Custom date formats
}

type DetailConfig struct {
	Enabled   bool                 `yaml:"enabled"`
	Selectors DetailSelectorConfig `yaml:"selectors,omitempty"`
	Parse     DetailParseConfig    `yaml:"parse,omitempty"`
}

type DetailSelectorConfig struct {
	Container   string `yaml:"container,omitempty"` // Wrapper for detail content
	Description string `yaml:"description,omitempty"`
	Deadline    string `yaml:"deadline,omitempty"`
	Amount      string `yaml:"amount,omitempty"`
	Eligibility string `yaml:"eligibility,omitempty"`
	Category    string `yaml:"category,omitempty"`
}

// LoadRegistry reads the embedded sources.yaml and returns a Registry.
// The path parameter is kept for backward compatibility but ignored.
func LoadRegistry(path string) (*Registry, error) {
	data, err := sourcesYAML.ReadFile("config/sources.yaml")
	if err != nil {
		// Fallback to filesystem for local development
		data, err = os.ReadFile(path)
		if err != nil {
			return nil, err
		}
	}

	// Expand environment variables within the YAML content (e.g. ${API_KEY})
	expanded := os.ExpandEnv(string(data))

	var reg Registry
	if err := yaml.Unmarshal([]byte(expanded), &reg); err != nil {
		return nil, err
	}

	return &reg, nil
}
