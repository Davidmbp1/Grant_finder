package main

import (
	"fmt"
	"net/http"
	"os"
	"strings"
)

func main() {
	adminSecret := strings.TrimSpace(os.Getenv("ADMIN_SECRET"))
	if adminSecret == "" {
		fmt.Println("Missing ADMIN_SECRET environment variable")
		os.Exit(1)
	}

	url := "http://localhost:8081/api/v1/ingest/source/prociencia_concursos_abiertos"
	req, err := http.NewRequest("POST", url, nil)
	if err != nil {
		fmt.Printf("Error creating request: %v\n", err)
		os.Exit(1)
	}
	req.Header.Set("X-Admin-Secret", adminSecret)
	req.Header.Set("Authorization", "Bearer "+adminSecret)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("Error sending request: %v\n", err)
		os.Exit(1)
	}
	defer resp.Body.Close()

	fmt.Printf("Response Status: %s\n", resp.Status)
	if resp.StatusCode != http.StatusOK {
		os.Exit(1)
	}
}
