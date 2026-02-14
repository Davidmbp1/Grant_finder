package main

import (
	"context"
	"log"
	"os"

	"github.com/david/grant-finder/internal/api"
	"github.com/david/grant-finder/internal/db"
)

func main() {
	port := os.Getenv("PORT")
	if port == "" {
		port = "8081"
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

	srv := api.NewServer(pool)
	log.Printf("Server starting on port %s...", port)
	if err := srv.Start(port); err != nil {
		log.Fatal(err)
	}
}
