package ingest

import (
	"fmt"
	"regexp"
	"strings"
	"time"
)

// parseDateRobust attempts to parse dates in multiple formats and locales
func parseDateRobust(text string, locales []string) (time.Time, error) {
	// Clean the text first
	text = cleanDateString(text)
	text = strings.ReplaceAll(text, "a.m.", "AM")
	text = strings.ReplaceAll(text, "p.m.", "PM")
	text = strings.ReplaceAll(text, "a.m", "AM")
	text = strings.ReplaceAll(text, "p.m", "PM")
	text = strings.ReplaceAll(text, " am", " AM")
	text = strings.ReplaceAll(text, " pm", " PM")

	// Try ISO format first (most reliable)
	if t, err := time.Parse(time.RFC3339, text); err == nil {
		return t, nil
	}
	if t, err := time.Parse("2006-01-02", text); err == nil {
		return toEndOfDay(t), nil
	}
	if t, err := time.Parse("2006-01-02T15:04:05Z", text); err == nil {
		return t, nil
	}

	// Try common English formats
	englishFormats := []string{
		"2 January 2006",
		"02 January 2006",
		"2 January 2006 3 PM",
		"2 January 2006 3:04 PM",
		"January 2, 2006",
		"January 2, 2006 3 PM",
		"January 2, 2006 3:04 PM",
		"Jan 2, 2006",
		"2 Jan 2006",
		"02 Jan 2006",
		"01/02/2006",
		"01/02/2006 3 PM",
		"02/01/2006", // UK format
		"2006-01-02 15:04:05",
	}

	for _, format := range englishFormats {
		if t, err := time.Parse(format, text); err == nil {
			// If format has time, return as is. If date only, end of day.
			if strings.Contains(format, ":") {
				return t, nil
			}
			return toEndOfDay(t), nil
		}
	}

	// Try Spanish formats if Spanish locale is specified
	for _, locale := range locales {
		if locale == "es" || strings.HasPrefix(locale, "es") {
			spanishFormats := []string{
				"2 de enero de 2006",
				"02 de enero de 2006",
				"2 de ene de 2006",
				"02/01/2006",
				"2-01-2006",
			}
			for _, format := range spanishFormats {
				if t, err := parseSpanishDate(text, format); err == nil {
					return toEndOfDay(t), nil
				}
			}
			// Try regex for Spanish dates (handles "del" and surrounding text)
			if t := parseSpanishDateWithRegex(text); !t.IsZero() {
				return toEndOfDay(t), nil
			}
		}
	}

	// Try regex-based parsing for common patterns
	if t := parseDateWithRegex(text); !t.IsZero() {
		return toEndOfDay(t), nil
	}

	return time.Time{}, fmt.Errorf("unable to parse date: %s", text)
}

// toEndOfDay sets the time to 23:59:59.999999999 UTC
func toEndOfDay(t time.Time) time.Time {
	return time.Date(t.Year(), t.Month(), t.Day(), 23, 59, 59, 999999999, time.UTC)
}

// parseSpanishDate handles Spanish date formats with month names
func parseSpanishDate(text, format string) (time.Time, error) {
	// Map Spanish months
	spanishMonths := map[string]string{
		"enero":      "January",
		"febrero":    "February",
		"marzo":      "March",
		"abril":      "April",
		"mayo":       "May",
		"junio":      "June",
		"julio":      "July",
		"agosto":     "August",
		"septiembre": "September",
		"octubre":    "October",
		"noviembre":  "November",
		"diciembre":  "December",
		"ene":        "Jan",
		"feb":        "Feb",
		"mar":        "Mar",
		"abr":        "Apr",
		"may":        "May",
		"jun":        "Jun",
		"jul":        "Jul",
		"ago":        "Aug",
		"sep":        "Sep",
		"oct":        "Oct",
		"nov":        "Nov",
		"dic":        "Dec",
	}

	textLower := strings.ToLower(text)
	for es, en := range spanishMonths {
		textLower = strings.ReplaceAll(textLower, es, en)
	}

	// Replace "de" with spaces
	textLower = strings.ReplaceAll(textLower, " de ", " ")

	// Try parsing with English month names
	englishFormat := strings.ReplaceAll(format, "enero", "January")
	englishFormat = strings.ReplaceAll(englishFormat, "ene", "Jan")
	englishFormat = strings.ReplaceAll(englishFormat, " de ", " ")

	return time.Parse(englishFormat, textLower)
}

// parseDateWithRegex uses regex to extract dates from text
func parseDateWithRegex(text string) time.Time {
	// ISO date: 2026-03-15
	isoRegex := regexp.MustCompile(`\b(20\d{2})-(\d{2})-(\d{2})\b`)
	if matches := isoRegex.FindStringSubmatch(text); len(matches) == 4 {
		if t, err := time.Parse("2006-01-02", matches[0]); err == nil {
			return t
		}
	}

	// US format: 03/15/2026 or 3/15/2026
	usRegex := regexp.MustCompile(`\b(\d{1,2})/(\d{1,2})/(20\d{2})\b`)
	if matches := usRegex.FindStringSubmatch(text); len(matches) == 4 {
		dateStr := fmt.Sprintf("%s/%s/%s", matches[1], matches[2], matches[3])
		if t, err := time.Parse("1/2/2006", dateStr); err == nil {
			return t
		}
		if t, err := time.Parse("01/02/2006", dateStr); err == nil {
			return t
		}
	}

	// UK/EU format: 15/03/2026
	ukRegex := regexp.MustCompile(`\b(\d{1,2})/(\d{1,2})/(20\d{2})\b`)
	if matches := ukRegex.FindStringSubmatch(text); len(matches) == 4 {
		dateStr := fmt.Sprintf("%s/%s/%s", matches[2], matches[1], matches[3]) // Swap day/month
		if t, err := time.Parse("01/02/2006", dateStr); err == nil {
			return t
		}
	}

	// Month name format: March 15, 2026 or 15 March 2026
	monthRegex := regexp.MustCompile(`\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|May|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+(\d{1,2}),?\s+(20\d{2})\b`)
	if matches := monthRegex.FindStringSubmatch(text); len(matches) == 4 {
		dateStr := fmt.Sprintf("%s %s %s", matches[1], matches[2], matches[3])
		if t, err := time.Parse("January 2, 2006", dateStr); err == nil {
			return t
		}
		if t, err := time.Parse("Jan 2, 2006", dateStr); err == nil {
			return t
		}
		if t, err := time.Parse("2 January 2006", dateStr); err == nil {
			return t
		}
		if t, err := time.Parse("2 Jan 2006", dateStr); err == nil {
			return t
		}
	}

	return time.Time{}
}

// parseSpanishDateWithRegex uses regex to extract Spanish dates from text
func parseSpanishDateWithRegex(text string) time.Time {
	// 17 de junio de 2025 OR 17 de junio del 2025
	// Regex matches: day, month, year
	spanishMonthRegex := regexp.MustCompile(`(?i)\b(\d{1,2})\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(?:de|del)\s+(20\d{2})\b`)

	if matches := spanishMonthRegex.FindStringSubmatch(text); len(matches) == 4 {
		day := matches[1]
		month := strings.ToLower(matches[2])
		year := matches[3]

		spanishMonths := map[string]string{
			"enero":      "January",
			"febrero":    "February",
			"marzo":      "March",
			"abril":      "April",
			"mayo":       "May",
			"junio":      "June",
			"julio":      "July",
			"agosto":     "August",
			"septiembre": "September",
			"octubre":    "October",
			"noviembre":  "November",
			"diciembre":  "December",
		}

		englishMonth, ok := spanishMonths[month]
		if ok {
			dateStr := fmt.Sprintf("%s %s %s", day, englishMonth, year)
			if t, err := time.Parse("2 January 2006", dateStr); err == nil {
				return t
			}
		}
	}
	return time.Time{}
}

// cleanDateString removes common prefixes and cleans up date strings
func cleanDateString(s string) string {
	prefixes := []string{
		"Closing date:", "Deadline:", "Open:", "Publication date:",
		"Fecha límite:", "Fecha de cierre:", "Cierre:",
		"Due date:", "Expires:", "Ends:",
	}
	sLower := strings.ToLower(s)
	for _, p := range prefixes {
		if idx := strings.Index(sLower, strings.ToLower(p)); idx != -1 {
			s = s[idx+len(p):]
			sLower = sLower[idx+len(p):]
		}
	}
	return strings.TrimSpace(s)
}
