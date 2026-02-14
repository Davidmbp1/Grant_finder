package ingest

import (
	"regexp"
	"strconv"
	"strings"
)

// parseAmountRobust extracts min/max amounts and currency from text with improved detection
func parseAmountRobust(text string, defaultCurrency string) (float64, float64, string) {
	textLower := strings.ToLower(text)

	// Currency detection (enhanced)
	currency := defaultCurrency
	if currency == "" {
		currency = "USD" // Default
	}

	// Check for currency symbols and codes
	if strings.Contains(textLower, "£") || strings.Contains(textLower, "gbp") || strings.Contains(textLower, "pound") {
		currency = "GBP"
	} else if strings.Contains(textLower, "€") || strings.Contains(textLower, "eur") || strings.Contains(textLower, "euro") {
		currency = "EUR"
	} else if strings.Contains(textLower, "$") || strings.Contains(textLower, "usd") || strings.Contains(textLower, "dollar") {
		currency = "USD"
	} else if strings.Contains(textLower, "peso") || strings.Contains(textLower, "mxn") {
		currency = "MXN"
	} else if strings.Contains(textLower, "sol") || strings.Contains(textLower, "pen") {
		currency = "PEN"
	}

	// Enhanced regex to find numbers with various separators
	// Handles: 1,000,000 / 1.000.000 / 1000000 / 1,000.50
	numberRegex := regexp.MustCompile(`[\d,\.]+(?:\.\d{2})?`)
	matches := numberRegex.FindAllString(text, -1)

	var amounts []float64
	for _, m := range matches {
		// Try parsing with comma as thousands separator
		clean := strings.ReplaceAll(m, ",", "")
		if val, err := strconv.ParseFloat(clean, 64); err == nil && val > 0 {
			amounts = append(amounts, val)
		} else {
			// Try with dot as thousands separator (European format)
			clean = strings.ReplaceAll(m, ".", "")
			if val, err := strconv.ParseFloat(clean, 64); err == nil && val > 0 {
				amounts = append(amounts, val)
			}
		}
	}

	if len(amounts) == 0 {
		return 0, 0, ""
	}

	if len(amounts) == 1 {
		// Single amount - check if it's "up to" or "minimum"
		if strings.Contains(textLower, "up to") || strings.Contains(textLower, "hasta") || strings.Contains(textLower, "maximum") {
			return 0, amounts[0], currency
		}
		if strings.Contains(textLower, "minimum") || strings.Contains(textLower, "at least") {
			return amounts[0], 0, currency
		}
		// Default: treat as maximum
		return 0, amounts[0], currency
	}

	// Multiple amounts - assume range
	// Sort to get min and max
	min := amounts[0]
	max := amounts[0]
	for _, a := range amounts {
		if a < min {
			min = a
		}
		if a > max {
			max = a
		}
	}

	// If min and max are the same, it's likely not a range
	if min == max && len(amounts) > 1 {
		// Take first two distinct values if available
		distinct := []float64{amounts[0]}
		for _, a := range amounts[1:] {
			if a != distinct[0] {
				distinct = append(distinct, a)
				if len(distinct) >= 2 {
					break
				}
			}
		}
		if len(distinct) >= 2 {
			if distinct[0] < distinct[1] {
				return distinct[0], distinct[1], currency
			}
			return distinct[1], distinct[0], currency
		}
		return 0, max, currency
	}

	return min, max, currency
}
