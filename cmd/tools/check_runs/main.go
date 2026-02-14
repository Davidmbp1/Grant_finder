package main

import (
	"context"
	"log"
	"os"
	"time"

	"github.com/david/grant-finder/internal/db"
	"github.com/jedib0t/go-pretty/v6/table"
)

func main() {
	ctx := context.Background()
	pool, err := db.Connect(ctx)
	if err != nil {
		log.Fatal(err)
	}
	defer pool.Close()

	rows, err := pool.Query(ctx, "SELECT run_id, source_id, status, items_found, items_saved, errors, started_at, completed_at FROM ingest_runs ORDER BY started_at DESC LIMIT 10")
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	t := table.NewWriter()
	t.SetOutputMirror(os.Stdout)
	t.AppendHeader(table.Row{"Source", "Status", "Found", "Saved", "Errors", "Duration", "Started At"})

	for rows.Next() {
		var runID, sourceID, status string
		var found, saved, errs int
		var startedAt time.Time
		var completedAt *time.Time

		if err := rows.Scan(&runID, &sourceID, &status, &found, &saved, &errs, &startedAt, &completedAt); err != nil {
			log.Printf("Scan error: %v", err)
			continue
		}

		duration := "Running..."
		if completedAt != nil {
			duration = completedAt.Sub(startedAt).Round(time.Second).String()
		}

		t.AppendRow(table.Row{sourceID, status, found, saved, errs, duration, startedAt.Format("15:04:05")})
	}
	t.Render()
}
