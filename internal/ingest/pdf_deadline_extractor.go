package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"regexp"
	"sort"
	"strings"
	"time"

	rpdf "rsc.io/pdf"
)

var deadlineLabelHints = []string{
	"inicio de postulaciones", "cierre de postulaciones", "fecha máxima", "deadline", "closes", "fecha límite", "cronograma", "calendario", "postulación",
}

var dateSnippetRegexes = []*regexp.Regexp{
	regexp.MustCompile(`(?i)\b\d{1,2}/\d{1,2}/20\d{2}\b`),
	regexp.MustCompile(`(?i)\b20\d{2}-\d{2}-\d{2}\b`),
	regexp.MustCompile(`(?i)\b\d{1,2}\s+de\s+(enero|febrero|marzo|abril|mayo|junio|julio|agosto|septiembre|octubre|noviembre|diciembre)\s+(de|del)\s+20\d{2}\b`),
	regexp.MustCompile(`(?i)\b\d{1,2}\s+(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+20\d{2}(\s+\d{1,2}(:\d{2})?\s*(a\.?m\.?|p\.?m\.?))?\b`),
	regexp.MustCompile(`(?i)\b(January|February|March|April|May|June|July|August|September|October|November|December|Jan|Feb|Mar|Apr|Jun|Jul|Aug|Sep|Oct|Nov|Dec)\s+\d{1,2},?\s+20\d{2}(\s+\d{1,2}(:\d{2})?\s*(a\.?m\.?|p\.?m\.?))?\b`),
}

func extractPDFText(content []byte) (text string, err error) {
	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("pdf parser panic: %v", recovered)
			text = ""
		}
	}()

	reader, err := rpdf.NewReader(bytes.NewReader(content), int64(len(content)))
	if err != nil {
		return "", err
	}

	var builder strings.Builder
	for pageIndex := 1; pageIndex <= reader.NumPage(); pageIndex++ {
		page := reader.Page(pageIndex)
		if page.V.IsNull() {
			continue
		}
		text := page.Content().Text
		for _, fragment := range text {
			builder.WriteString(fragment.S)
			builder.WriteString(" ")
		}
		builder.WriteString("\n")
	}

	return builder.String(), nil
}

func parseDateCandidatesFromText(text string) []string {
	evidence := parseDeadlineEvidenceFromText(text, "text", "", 0.7)
	if len(evidence) == 0 {
		return nil
	}

	result := make([]string, 0, len(evidence))
	seen := make(map[string]bool)
	for _, ev := range evidence {
		iso := ev.ParsedDateISO
		if !seen[iso] {
			result = append(result, iso)
			seen[iso] = true
		}
	}

	return result
}

func parseDeadlineEvidenceFromText(text, source, sourceURL string, defaultConfidence float64) []DeadlineEvidence {
	matches := make(map[string]DeadlineEvidence)
	locales := []string{"en", "es"}

	for _, expr := range dateSnippetRegexes {
		for _, loc := range expr.FindAllStringIndex(text, -1) {
			token := strings.TrimSpace(text[loc[0]:loc[1]])
			parsed, err := parseDateRobust(token, locales)
			if err != nil {
				continue
			}
			if !hasExplicitTimeToken(token) {
				parsed = normalizeDateOnlyBySource(parsed, sourceURL)
			}
			iso := parsed.UTC().Format(time.RFC3339)
			start := loc[0] - 80
			if start < 0 {
				start = 0
			}
			end := loc[1] + 80
			if end > len(text) {
				end = len(text)
			}
			snippet := strings.TrimSpace(strings.ReplaceAll(text[start:end], "\n", " "))
			label := "deadline"
			snippetLower := strings.ToLower(snippet)
			for _, hint := range deadlineLabelHints {
				if strings.Contains(snippetLower, hint) {
					label = hint
					break
				}
			}
			matches[iso] = DeadlineEvidence{
				Source:        source,
				URL:           sourceURL,
				Snippet:       snippet,
				ParsedDateISO: iso,
				Label:         label,
				Confidence:    defaultConfidence,
			}
		}
	}

	if len(matches) == 0 {
		return nil
	}

	ordered := make([]DeadlineEvidence, 0, len(matches))
	for _, ev := range matches {
		ordered = append(ordered, ev)
	}
	sort.Slice(ordered, func(i, j int) bool {
		ti, _ := time.Parse(time.RFC3339, ordered[i].ParsedDateISO)
		tj, _ := time.Parse(time.RFC3339, ordered[j].ParsedDateISO)
		return ti.Before(tj)
	})

	return ordered
}

func hasExplicitTimeToken(token string) bool {
	lower := strings.ToLower(token)
	if strings.Contains(lower, ":") {
		return true
	}
	timeHints := []string{" am", " pm", "a.m", "p.m", "utc", "gmt", "hora", "hrs"}
	for _, hint := range timeHints {
		if strings.Contains(lower, hint) {
			return true
		}
	}
	return false
}

func normalizeDateOnlyBySource(parsed time.Time, sourceURL string) time.Time {
	loc := time.UTC
	lowerURL := strings.ToLower(sourceURL)
	if strings.Contains(lowerURL, "gob.pe") || strings.Contains(lowerURL, "proinnovate") || strings.Contains(lowerURL, "prociencia") {
		if lima, err := time.LoadLocation("America/Lima"); err == nil {
			loc = lima
		}
	}
	localized := time.Date(parsed.Year(), parsed.Month(), parsed.Day(), 23, 59, 59, 0, loc)
	return localized.UTC()
}

func extractDeadlinesFromPDF(ctx context.Context, fetcher Fetcher, pdfURL string) ([]string, string, error) {
	doc, err := fetcher.Fetch(ctx, pdfURL)
	if err != nil {
		return nil, "", err
	}
	defer doc.Body.Close()

	pdfContent, err := io.ReadAll(doc.Body)
	if err != nil {
		return nil, "", fmt.Errorf("pdf read failed: %w", err)
	}

	text, err := extractPDFText(pdfContent)
	if err != nil {
		return nil, "", fmt.Errorf("pdf text extraction failed: %w", err)
	}

	deadlines := parseDateCandidatesFromText(text)
	return deadlines, text, nil
}
