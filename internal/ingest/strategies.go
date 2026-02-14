package ingest

import (
	"context"
	"fmt"
)

// IngestionStats holds metrics about a run
type IngestionStats struct {
	TotalSaved int
	TotalFound int
	Errors     int
}

// FetcherStrategy defines the contract for any ingestion source.
// It handles fetching, parsing, and saving via the provided pipeline callback.
type FetcherStrategy interface {
	// Run executes the ingestion process for a specific source configuration.
	// It uses the pipeline to save opportunities and access shared resources (DB, AI).
	Run(ctx context.Context, config SourceConfig, pipeline *Pipeline) (IngestionStats, error)
}

// StrategyFactory maps strategy IDs (from sources.yaml) to implementations.
type StrategyFactory struct {
	strategies map[string]FetcherStrategy
}

func NewStrategyFactory() *StrategyFactory {
	return &StrategyFactory{
		strategies: make(map[string]FetcherStrategy),
	}
}

func (f *StrategyFactory) Register(id string, strategy FetcherStrategy) {
	f.strategies[id] = strategy
}

func (f *StrategyFactory) Get(id string) (FetcherStrategy, error) {
	strategy, ok := f.strategies[id]
	if !ok {
		return nil, fmt.Errorf("strategy not found: %s", id)
	}
	return strategy, nil
}

// Global factory instance
var GlobalStrategyFactory = NewStrategyFactory()

func init() {
	// Register default strategies here or in their respective files
	GlobalStrategyFactory.Register("api_grants_gov", &GrantsGovStrategy{})
	GlobalStrategyFactory.Register("api_eu_ft", &EuFundingTendersStrategy{})
	GlobalStrategyFactory.Register("html_generic", &HtmlGenericStrategy{})
	GlobalStrategyFactory.Register("wordpress_rest", &WordPressStrategy{})
}
