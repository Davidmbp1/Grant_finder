package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"strings"
)

// ExtractedData represents the structured output from the LLM.
type ExtractedData struct {
	DeadlineText string   `json:"deadline_text"`
	DeadlineISO  string   `json:"deadline_iso"`
	OpenDateISO  string   `json:"open_date_iso"`
	OpenISO      string   `json:"open_iso"`
	CloseISO     string   `json:"close_iso"`
	ExpirationISO string  `json:"expiration_iso"`
	DeadlineCandidates []string `json:"deadline_candidates"`
	SourceStatusRaw string `json:"source_status_raw"`
	IsResultsPage bool    `json:"is_results_page"`
	IsRolling    bool     `json:"is_rolling"`
	OppStatus    string   `json:"opp_status"` // posted, closed, archived, funded
	AmountMin    float64  `json:"amount_min"`
	AmountMax    float64  `json:"amount_max"`
	Currency     string   `json:"currency"`
	Eligibility  string   `json:"eligibility"`
	Categories   []string `json:"categories"`
	Summary      string   `json:"summary"`
}

// ExtractOpportunityData uses the LLM to extract structured data from text.
func (c *OllamaClient) ExtractOpportunityData(ctx context.Context, title, url, text string) (*ExtractedData, error) {
	prompt := fmt.Sprintf(`You are an expert grant analyst. Extract key information from the following grant opportunity text into JSON format.

Input:
Title: %s
URL: %s
Text:
%s

Instructions:
1. Extract date candidates in deadline_candidates (ISO 8601 YYYY-MM-DD), including multiple receipt/cycle deadlines if present.
2. If a main deadline is obvious, also fill deadline_iso.
3. Extract source_status_raw exactly as text seen in source (examples: "open", "closed", "results", "winners announced", "finalizado").
4. Extract is_results_page=true if this page is clearly results/winners/ranking/historical, else false.
5. Extract open_iso / close_iso / expiration_iso when explicitly present.
6. If descriptions like "until funds exhausted", "open all year", or "ventanilla abierta", set is_rolling=true.
3. Extract AMOUNT. amount_min and amount_max in numeric. currency in 3-letter ISO code (e.g. USD, PEN, EUR, GBP, CAD).
4. Summary: Write a 1-2 sentence neutral summary.
5. Categories: List 1-3 tags (e.g. "Research", "Innovation", "Scholarship").

JSON Schema:
{
	"deadline_text": "string or null",
	"deadline_iso": "YYYY-MM-DD or null",
	"deadline_candidates": ["YYYY-MM-DD"],
	"source_status_raw": "string or null",
	"is_results_page": false,
	"open_date_iso": "YYYY-MM-DD or null",
	"open_iso": "YYYY-MM-DD or null",
	"close_iso": "YYYY-MM-DD or null",
	"expiration_iso": "YYYY-MM-DD or null",
	"is_rolling": boolean,
	"opp_status": "posted" | "closed" | "archived" | "funded",
	"amount_min": number,
	"amount_max": number,
	"currency": "3-letter ISO code (e.g. USD, PEN) or null",
	"eligibility": "string",
	"categories": ["string"],
	"summary": "string"
}

Respond ONLY with the JSON object.`, title, url, text)

	// Strategy: Try with jsonMode=true first (better adherence for models that support it)
	// If that fails (or returns non-JSON), fallback to text mode + robust extraction

	// Attempt 1: JSON Mode
	resp, err := c.GenerateCompletion(ctx, prompt, true)
	if err == nil {
		if data, parseErr := parseLLMResponse(resp); parseErr == nil {
			return data, nil
		} else {
			log.Printf("JSON mode failed parsing: %v. Retrying with text mode...", parseErr)
		}
	} else {
		log.Printf("JSON mode generation failed: %v. Retrying with text mode...", err)
	}

	// Attempt 2: Text Mode (Robust fallback)
	resp, err = c.GenerateCompletion(ctx, prompt, false)
	if err != nil {
		return nil, err
	}

	// Debug: Log raw response from fallback
	log.Printf("DEBUG LLM RESP (Text Mode): %s\n", resp)

	data, err := parseLLMResponse(resp)
	if err != nil {
		return nil, fmt.Errorf("failed to parse LLM JSON after retry: %w (response: %s)", err, resp)
	}

	return data, nil
}

func parseLLMResponse(resp string) (*ExtractedData, error) {
	// Clean markdown code blocks
	cleaned := strings.TrimSpace(resp)
	cleaned = strings.TrimPrefix(cleaned, "```json")
	cleaned = strings.TrimPrefix(cleaned, "```")
	cleaned = strings.TrimSuffix(cleaned, "```")

	// Extract first valid JSON object {...}
	if jsonStr, ok := extractFirstJSONObject(cleaned); ok {
		cleaned = jsonStr
	}

	var data ExtractedData
	if err := json.Unmarshal([]byte(cleaned), &data); err != nil {
		return nil, err
	}
	return &data, nil
}

// extractFirstJSONObject finds the first outermost balanced {...}
func extractFirstJSONObject(s string) (string, bool) {
	start := strings.Index(s, "{")
	if start == -1 {
		return "", false
	}

	depth := 0
	inString := false
	escaped := false

	for i := start; i < len(s); i++ {
		char := s[i]

		if escaped {
			escaped = false
			continue
		}

		if char == '\\' {
			escaped = true
			continue
		}

		if char == '"' {
			inString = !inString
			continue
		}

		if !inString {
			if char == '{' {
				depth++
			} else if char == '}' {
				depth--
				if depth == 0 {
					return s[start : i+1], true
				}
			}
		}
	}

	return "", false
}
