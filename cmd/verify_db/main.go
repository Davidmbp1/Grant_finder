package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"github.com/jackc/pgx/v5/pgxpool"
)

func main() {
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		dbURL = "postgres://postgres:password@127.0.0.1:5440/grant_finder?sslmode=disable"
	}

	db, err := pgxpool.New(context.Background(), dbURL)
	if err != nil {
		log.Fatalf("Unable to connect to database: %v", err)
	}
	defer db.Close()

	var count, sourceIDCount, eligibilityCount, descCount int
	err = db.QueryRow(context.Background(), `
		SELECT 
			count(*), 
			count(source_id), 
			count(eligibility),
			count(description_html)
		FROM opportunities 
		WHERE source_domain = 'grants.gov'
	`).Scan(&count, &sourceIDCount, &eligibilityCount, &descCount)

	if err != nil {
		log.Fatalf("Query failed: %v", err)
	}

	fmt.Printf("Total Grants.gov: %d\n", count)
	fmt.Printf("With SourceID: %d\n", sourceIDCount)
	fmt.Printf("With Eligibility: %d\n", eligibilityCount)
	fmt.Printf("With Description: %d\n", descCount)
}
