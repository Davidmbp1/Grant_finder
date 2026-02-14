package ai

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
)

// AnalyzeStatus uses the LLM to determine if a grant is open, closed, or forthcoming
// based on its text content. This is useful for ambiguous cases where no date is present.
func AnalyzeStatus(ctx context.Context, client *OllamaClient, title, summary string) (string, error) {
	prompt := fmt.Sprintf(`You are an expert grant analyst. Determine the status of this grant opportunity based on the text below.

GRANT TITLE: %s
GRANT SUMMARY: %s

Is this grant currently open for applications?
- If the text explicitly says "closed", "expired", "past", "no longer accepting", or similar, return "closed".
- If the text mentions a past year (e.g. 2020, 2023) and no future year, return "closed".
- If the text says "coming soon", "future", "anticipated", return "forthcoming".
- If it seems active, open, or rolling, return "posted".

Return ONLY a JSON object:
{
  "status": "posted" | "closed" | "forthcoming",
  "reason": "brief explanation"
}
`, title, summary)

	resp, err := client.GenerateCompletion(ctx, prompt, true)
	if err != nil {
		return "posted", err // Default to posted on error to be safe
	}

	var result struct {
		Status string `json:"status"`
		Reason string `json:"reason"`
	}

	if err := json.Unmarshal([]byte(resp), &result); err != nil {
		return "posted", fmt.Errorf("failed to parse status json: %w", err)
	}

	status := strings.ToLower(strings.TrimSpace(result.Status))
	switch status {
	case "closed", "expired", "archived":
		return "closed", nil
	case "forthcoming", "upcoming":
		return "forthcoming", nil
	case "posted", "open", "active":
		return "posted", nil
	}

	return "posted", nil
}
