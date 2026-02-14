package ingest

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"
	"time"
)

// OllamaParser uses a local Ollama LLM to extract grant data from HTML/text.
type OllamaParser struct {
	BaseURL string // e.g. "http://localhost:11434"
	Model   string // e.g. "qwen2.5:14b"
	Client  *http.Client
}

func NewOllamaParser(model string) *OllamaParser {
	return &OllamaParser{
		BaseURL: "http://localhost:11434",
		Model:   model,
		Client: &http.Client{
			Timeout: 120 * time.Second, // LLM can be slow
		},
	}
}

type ollamaRequest struct {
	Model  string `json:"model"`
	Prompt string `json:"prompt"`
	Stream bool   `json:"stream"`
	Format string `json:"format"`
}

type ollamaResponse struct {
	Response string `json:"response"`
}

type llmGrantOutput struct {
	Title       string  `json:"title"`
	Summary     string  `json:"summary"`
	Description string  `json:"description"`
	AmountMin   float64 `json:"amount_min"`
	AmountMax   float64 `json:"amount_max"`
	Currency    string  `json:"currency"`
	Deadline    string  `json:"deadline"`
	IsRolling   bool    `json:"is_rolling"`
	Category    string  `json:"category"`
	Type        string  `json:"type"`
	URL         string  `json:"url"`
}

const extractionPrompt = `You are a grant data extraction assistant. Given the following webpage text, extract ALL funding opportunities mentioned. 

For EACH opportunity, return a JSON object in an array with these fields:
- "title": the grant/funding name.
- "summary": a concise 1-2 sentence overview for list views.
- "description": a DETAILED description (3-5 paragraphs) extracting all available info on objectives, scope, eligibility, and requirements. Format as Markdown.
- "amount_min": minimum funding amount as a number (0 if unknown).
- "amount_max": maximum funding amount as a number (0 if unknown).
- "currency": ISO currency code (USD, EUR, GBP, etc.).
- "deadline": ISO 8601 date string or "" if unknown.
- "is_rolling": true if applications are accepted on a rolling basis.
- "category": one of: health, climate, technology, education, culture, social, innovation, research, other.
- "type": one of: grant, fellowship, prize, award, consultancy, accelerator, other.
- "url": the DIRECT application URL or specific detail page. Do NOT return the homepage unless it's the only link.

IMPORTANT RULES:
- Return ONLY a valid JSON array. No markdown blocks, no explanation.
- If no field is found, use 0 for numbers, "" for strings, false for booleans.
- Do NOT invent data. Only extract what is explicitly stated, but be comprehensive with the description.

WEBPAGE TEXT:
%s`

func (p *OllamaParser) Parse(ctx context.Context, r io.Reader) ([]Opportunity, error) {
	// Read all content
	bodyBytes, err := io.ReadAll(r)
	if err != nil {
		return nil, fmt.Errorf("reading body: %w", err)
	}

	text := string(bodyBytes)
	// Truncate if too long (LLM context limit)
	if len(text) > 12000 {
		text = text[:12000]
	}

	prompt := fmt.Sprintf(extractionPrompt, text)

	reqBody := ollamaRequest{
		Model:  p.Model,
		Prompt: prompt,
		Stream: false,
		Format: "json",
	}

	jsonBody, err := json.Marshal(reqBody)
	if err != nil {
		return nil, fmt.Errorf("marshaling request: %w", err)
	}

	log.Printf("[OllamaParser] Sending %d chars to %s...", len(text), p.Model)

	req, err := http.NewRequestWithContext(ctx, "POST", p.BaseURL+"/api/generate", bytes.NewReader(jsonBody))
	if err != nil {
		return nil, fmt.Errorf("creating request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := p.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("ollama request failed: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		respBody, _ := io.ReadAll(resp.Body)
		return nil, fmt.Errorf("ollama returned %d: %s", resp.StatusCode, string(respBody))
	}

	var ollamaResp ollamaResponse
	if err := json.NewDecoder(resp.Body).Decode(&ollamaResp); err != nil {
		return nil, fmt.Errorf("decoding ollama response: %w", err)
	}

	log.Printf("[OllamaParser] LLM response: %d chars", len(ollamaResp.Response))

	// Parse the LLM JSON output
	responseText := strings.TrimSpace(ollamaResp.Response)

	// Try parsing as array first
	var grants []llmGrantOutput
	if err := json.Unmarshal([]byte(responseText), &grants); err != nil {
		// Maybe it's a single object, wrap in array
		var single llmGrantOutput
		if err2 := json.Unmarshal([]byte(responseText), &single); err2 != nil {
			return nil, fmt.Errorf("failed to parse LLM JSON: %w\nRaw response: %s", err, responseText[:min(500, len(responseText))])
		}
		grants = []llmGrantOutput{single}
	}

	// Convert to our Opportunity type
	var opportunities []Opportunity
	for _, g := range grants {
		if g.Title == "" {
			continue
		}

		opp := Opportunity{
			Title:       g.Title,
			Summary:     g.Summary,
			Description: g.Description, // Mapped Description
			ExternalURL: g.URL,
			AmountMin:   g.AmountMin,
			AmountMax:   g.AmountMax,
			Currency:    g.Currency,
			IsRolling:   g.IsRolling,
			Category:    g.Category,
			Type:        g.Type,
		}

		// Parse deadline
		if g.Deadline != "" {
			if t, err := time.Parse(time.RFC3339, g.Deadline); err == nil {
				opp.DeadlineAt = &t
			} else if t, err := time.Parse("2006-01-02", g.Deadline); err == nil {
				opp.DeadlineAt = &t
			}
			opp.DeadlineStr = g.Deadline
		}

		if opp.Currency == "" {
			opp.Currency = "USD"
		}

		opportunities = append(opportunities, opp)
	}

	log.Printf("[OllamaParser] Extracted %d opportunities", len(opportunities))
	return opportunities, nil
}
