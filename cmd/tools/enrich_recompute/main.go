package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"os"
	"strings"
	"time"

	"github.com/david/grant-finder/internal/db"
	"github.com/david/grant-finder/internal/ingest"
)

type domainResult struct {
	Domain          string `json:"domain"`
	ItemsScanned    int    `json:"items_scanned"`
	ItemsUpdated    int    `json:"items_updated"`
	PDFsParsed      int    `json:"pdfs_parsed"`
	DeadlinesAdded  int    `json:"deadlines_added"`
	StatusChanges   int    `json:"status_changes"`
	OnlyMissing     bool   `json:"only_missing_deadlines"`
	BatchSize       int    `json:"batch_size"`
	MaxItems        int    `json:"max_items"`
	Threshold       float64 `json:"confidence_threshold"`
	Error           string `json:"error,omitempty"`
}

type output struct {
	Domains       []domainResult `json:"domains"`
	StatusUpdated int            `json:"status_updated"`
	StatusCounts  map[string]int `json:"status_counts"`
}

func main() {
	domainsCSV := flag.String("domains", "prociencia.gob.pe,calendario.proinnovate.gob.pe,cambioclimatico.proinnovate.gob.pe,startup.proinnovate.gob.pe,www.gob.pe", "comma-separated domains")
	onlyMissing := flag.Bool("only-missing-deadlines", false, "enrich only missing deadlines")
	batchSize := flag.Int("batch-size", 300, "batch size")
	maxItems := flag.Int("max-items", 2000, "max items per domain")
	threshold := flag.Float64("confidence-threshold", 0.6, "status confidence threshold")
	recomputeBatch := flag.Int("recompute-batch", 500, "recompute status batch size")
	perDomainTimeoutSec := flag.Int("domain-timeout-sec", 180, "timeout per domain enrichment")
	flag.Parse()

	ctx := context.Background()
	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatalf("db connect failed: %v", err)
	}
	defer pool.Close()

	if err := db.ApplyMigrations(ctx, pool); err != nil {
		log.Fatalf("migrations failed: %v", err)
	}

	pipeline := ingest.NewPipeline(pool, nil, nil, nil)
	result := output{Domains: []domainResult{}}

	for _, raw := range strings.Split(*domainsCSV, ",") {
		domain := strings.TrimSpace(raw)
		if domain == "" {
			continue
		}
		domainCtx, cancel := context.WithTimeout(ctx, time.Duration(*perDomainTimeoutSec)*time.Second)
		stats, err := pipeline.EnrichOpportunities(domainCtx, domain, *onlyMissing, *batchSize, *maxItems, *threshold)
		cancel()

		domainErr := ""
		if err != nil {
			domainErr = err.Error()
		}
		result.Domains = append(result.Domains, domainResult{
			Domain:         domain,
			ItemsScanned:   stats.ItemsScanned,
			ItemsUpdated:   stats.ItemsUpdated,
			PDFsParsed:     stats.PDFsParsed,
			DeadlinesAdded: stats.DeadlinesAdded,
			StatusChanges:  stats.StatusChanges,
			OnlyMissing:    *onlyMissing,
			BatchSize:      *batchSize,
			MaxItems:       *maxItems,
			Threshold:      *threshold,
			Error:          domainErr,
		})
	}

	statusCounts, updated, err := pipeline.RecomputeStatuses(ctx, *recomputeBatch)
	if err != nil {
		log.Fatalf("recompute failed: %v", err)
	}
	result.StatusUpdated = updated
	result.StatusCounts = statusCounts

	enc := json.NewEncoder(os.Stdout)
	enc.SetIndent("", "  ")
	if err := enc.Encode(result); err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
}
