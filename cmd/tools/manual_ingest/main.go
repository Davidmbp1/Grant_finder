package main

import (
	"context"
	"flag"
	"log"

	"github.com/david/grant-finder/internal/db"
	"github.com/david/grant-finder/internal/ingest"
)

func main() {
	sourceID := flag.String("source", "", "Source ID to ingest (e.g., ukri_uk)")
	flag.Parse()

	if *sourceID == "" {
		log.Fatal("Please provide a source ID using -source flag")
	}

	ctx := context.Background()
	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("Failed to connect to database: %v", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		log.Fatalf("Migration failed: %v", err)
	}

	// Initialize Pipeline
	// We pass nil for parser and AI because HtmlGenericStrategy doesn't strictly need them
	// (unless we want embeddings, but for verification of scraping/selectors, it's fine)
	// Actually, SaveOpportunity might try to embed if AI is present. We can pass nil to skip it.
	pipeline := ingest.NewPipeline(pool, nil, nil, nil)

	log.Printf("Starting manual ingestion for source: %s", *sourceID)
	stats, err := pipeline.IngestSource(ctx, *sourceID)
	if err != nil {
		log.Fatalf("Ingestion failed: %v", err)
	}

	log.Printf("Ingestion finished for %s. Found: %d, Saved: %d, Errors: %d", *sourceID, stats.TotalFound, stats.TotalSaved, stats.Errors)
}
