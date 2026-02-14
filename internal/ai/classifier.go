package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

type ClassificationResult struct {
	Categories  []string `json:"categories"`
	Eligibility []string `json:"eligibility"`
}

func ClassifyGrant(ctx context.Context, client *OllamaClient, title, summary string) (*ClassificationResult, error) {
	cats := strings.Join(Categories, ", ")
	elig := strings.Join(Eligibility, ", ")

	prompt := fmt.Sprintf(`You are an expert grant classifier. Your task is to categorize the following grant opportunity based on its Title and Summary.

GRANT TITLE: %s
GRANT SUMMARY: %s

Select the most relevant tags from the following EXACT lists. Do not invent new tags.

AVAILABLE CATEGORIES: %s
AVAILABLE ELIGIBILITY: %s

Return a JSON object with this format:
{
  "categories": ["Category1", "Category2"],
  "eligibility": ["Eligibility1", "Eligibility2"]
}

Rules:
1. Select only tags that strongly apply.
2. If the grant mentions "small business", tag "For-profit Business" and "Startups" if applicable.
3. If the grant mentions "school district", tag "Government".
4. If no tags apply, return empty arrays.
5. RESPOND ONLY WITH JSON.`, title, summary, cats, elig)

	resp, err := client.GenerateCompletion(ctx, prompt, true)
	if err != nil {
		return nil, err
	}

	// Clean response if necessary (sometimes models add text before/after even in JSON mode, though Ollama JSON mode is strict)
	// But `resp` should be the JSON string.
	var result ClassificationResult
	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return nil, fmt.Errorf("failed to parse classification json: %w. Response: %s", err, resp)
	}

	// Validate tags (optional, but good practice to filter out hallucinations)
	result.Categories = filterValid(result.Categories, Categories)
	result.Eligibility = filterValid(result.Eligibility, Eligibility)

	return &result, nil
}

func filterValid(tags []string, allowed []string) []string {
	valid := make([]string, 0)
	allowedMap := make(map[string]bool)
	for _, a := range allowed {
		allowedMap[a] = true
	}

	for _, t := range tags {
		// Fuzzy match? Or exact? Ollama usually respects exact if told so.
		// Let's try exact first.
		if allowedMap[t] {
			valid = append(valid, t)
		} else {
			// Try case-insensitive?
			for a := range allowedMap {
				if strings.EqualFold(a, t) {
					valid = append(valid, a) // Store the canonical one
					break
				}
			}
		}
	}
	return valid
}
