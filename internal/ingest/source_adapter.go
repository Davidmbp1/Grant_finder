package ingest

import (
	"context"
	"fmt"
	"io"
	"net/url"
	"regexp"
	"strings"
	"time"

	"github.com/PuerkitoBio/goquery"
)

type SourceAdapterRaw struct {
	URL             string
	Domain          string
	BodyHTML        string
	AttachmentURLs  []string
	AttachmentTexts map[string]string
	FetchMeta       map[string]interface{}
}

type SourceAdapterCandidates struct {
	SourceStatusRaw   string
	DeadlineCandidates []string
	DeadlineEvidence  []DeadlineEvidence
	OpenISO           string
	CloseISO          string
	ExpirationISO     string
	IsResultsPage     bool
	Evidence          map[string]interface{}
	StatusConfidence  float64
	RollingEvidence   bool
	PDFsParsed        int
	DeadlinesAdded    int
}

type SourceAdapter interface {
	FetchOpportunityRaw(ctx context.Context, idOrURL string) (*SourceAdapterRaw, error)
	ExtractCandidates(raw *SourceAdapterRaw) (*SourceAdapterCandidates, error)
}

type GenericSourceAdapter struct {
	Fetcher Fetcher
}

var attachmentAnchorRegex = regexp.MustCompile(`(?i)(calendar|schedule|timeline|dates|deadlines|guidelines|bases|cronograma|calendario|fechas|anexos|annex|attachments?)`)

func NewGenericSourceAdapter(fetcher Fetcher) *GenericSourceAdapter {
	return &GenericSourceAdapter{Fetcher: fetcher}
}

func (a *GenericSourceAdapter) FetchOpportunityRaw(ctx context.Context, idOrURL string) (*SourceAdapterRaw, error) {
	start := time.Now()
	doc, err := a.Fetcher.Fetch(ctx, idOrURL)
	if err != nil {
		return nil, err
	}
	defer doc.Body.Close()

	payload, err := io.ReadAll(doc.Body)
	if err != nil {
		return nil, err
	}
	fetchMeta := map[string]interface{}{
		"root_status_code": doc.StatusCode,
		"root_bytes":       len(payload),
		"root_duration_ms": time.Since(start).Milliseconds(),
		"blocked_detected": false,
	}

	htmlBody := string(payload)
	attachmentURLs := collectAttachmentPDFLinks(idOrURL, htmlBody)
	attachmentTexts := map[string]string{}
	pdfParseErrors := 0

	for _, attachmentURL := range attachmentURLs {
		attachmentStart := time.Now()
		doc, err := a.Fetcher.Fetch(ctx, attachmentURL)
		if err != nil {
			pdfParseErrors++
			continue
		}
		contentType := strings.ToLower(doc.ContentType)
		doc.Body.Close()
		if !strings.Contains(contentType, "pdf") && !strings.Contains(strings.ToLower(attachmentURL), ".pdf") {
			continue
		}
		_, text, err := extractDeadlinesFromPDF(ctx, a.Fetcher, attachmentURL)
		if err != nil {
			pdfParseErrors++
			continue
		}
		attachmentTexts[attachmentURL] = text
		fetchMeta[fmt.Sprintf("pdf_%s_duration_ms", attachmentURL)] = time.Since(attachmentStart).Milliseconds()
	}
	fetchMeta["attachment_count"] = len(attachmentURLs)
	fetchMeta["pdfs_parsed"] = len(attachmentTexts)
	fetchMeta["pdf_parse_errors"] = pdfParseErrors
	fetchMeta["pdf_unparseable"] = pdfParseErrors > 0

	return &SourceAdapterRaw{
		URL:             idOrURL,
		Domain:          extractDomain(idOrURL),
		BodyHTML:        htmlBody,
		AttachmentURLs:  attachmentURLs,
		AttachmentTexts: attachmentTexts,
		FetchMeta:       fetchMeta,
	}, nil
}

func (a *GenericSourceAdapter) ExtractCandidates(raw *SourceAdapterRaw) (*SourceAdapterCandidates, error) {
	text := strings.ToLower(buildStructuredExtractionText(raw.BodyHTML))
	htmlEvidence := parseDeadlineEvidenceFromText(text, "html", raw.URL, 0.8)
	htmlCandidates := parseDateCandidatesFromText(text)
	candidates := make([]string, 0, len(htmlCandidates))
	candidates = append(candidates, htmlCandidates...)
	deadlineEvidence := make([]DeadlineEvidence, 0, len(htmlEvidence))
	deadlineEvidence = append(deadlineEvidence, htmlEvidence...)
	evidence := map[string]interface{}{
		"authority":         "inference",
		"attachment_urls":   raw.AttachmentURLs,
		"attachment_count":  len(raw.AttachmentURLs),
		"rolling_evidence":  false,
		"evidence_snippets": []string{},
		"fetch_meta":        raw.FetchMeta,
	}

	rollingEvidence := false
	for _, hint := range []string{"rolling", "open continuously", "ongoing call", "ventanilla abierta", "convocatoria permanente", "sin fecha límite", "no deadline"} {
		if strings.Contains(text, hint) {
			rollingEvidence = true
			evidence["rolling_evidence"] = true
			break
		}
	}

	attachmentCandidatesFound := false
	pdfsParsed := 0
	for _, attachmentText := range raw.AttachmentTexts {
		pdfsParsed++
		before := len(candidates)
		candidates = mergeUniqueFold(candidates, parseDateCandidatesFromText(strings.ToLower(attachmentText)))
		pdfEvidence := parseDeadlineEvidenceFromText(strings.ToLower(attachmentText), "pdf", raw.URL, 0.85)
		deadlineEvidence = append(deadlineEvidence, pdfEvidence...)
		if len(candidates) > before {
			attachmentCandidatesFound = true
		}
	}

	statusRaw := ""
	if strings.Contains(text, "closed") || strings.Contains(text, "cerrad") || strings.Contains(text, "finalizada") {
		statusRaw = "closed"
	}
	if strings.Contains(text, "results") || strings.Contains(text, "winners") || strings.Contains(text, "ganadores") || strings.Contains(text, "resultados finales") || strings.Contains(text, "ranking") {
		statusRaw = "results"
	}
	if statusRaw == "" && (strings.Contains(text, "open") || strings.Contains(text, "abierta") || strings.Contains(text, "vigente")) {
		statusRaw = "open"
	}

	isResults := statusRaw == "results"
	confidence := 0.4
	if len(htmlCandidates) > 0 {
		confidence = 0.8
		evidence["authority"] = "official_page_html"
	}
	if attachmentCandidatesFound && len(htmlCandidates) == 0 {
		confidence = 0.7
		evidence["authority"] = "attachments"
	}
	if isResults {
		confidence = 0.95
	}

	if strings.Contains(strings.ToLower(raw.Domain), "proinnovate") && len(candidates) == 0 {
		evidence["proinnovate_discovery_only"] = true
		if statusRaw == "" {
			statusRaw = "calendar_discovery_only"
		}
		if confidence > 0.3 {
			confidence = 0.3
		}
	}

	return &SourceAdapterCandidates{
		SourceStatusRaw:    statusRaw,
		DeadlineCandidates: candidates,
		DeadlineEvidence:   deadlineEvidence,
		IsResultsPage:      isResults,
		Evidence:           evidence,
		StatusConfidence:   confidence,
		RollingEvidence:    rollingEvidence,
		PDFsParsed:         pdfsParsed,
		DeadlinesAdded:     len(candidates),
	}, nil
}

func collectAttachmentPDFLinks(baseURL, htmlBody string) []string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlBody))
	if err != nil {
		return nil
	}

	baseParsed, _ := url.Parse(baseURL)
	seen := map[string]bool{}
	var out []string

	doc.Find("a[href]").Each(func(_ int, sel *goquery.Selection) {
		href, ok := sel.Attr("href")
		if !ok {
			return
		}
		hrefLower := strings.ToLower(strings.TrimSpace(href))
		anchorText := strings.TrimSpace(strings.ToLower(sel.Text()))
		isLikelyDoc := attachmentAnchorRegex.MatchString(anchorText) || strings.Contains(hrefLower, ".pdf") || strings.Contains(hrefLower, "download") || strings.Contains(hrefLower, "/document/")
		if !isLikelyDoc {
			return
		}

		ref, err := url.Parse(strings.TrimSpace(href))
		if err != nil {
			return
		}
		abs := baseParsed.ResolveReference(ref).String()
		if !seen[abs] {
			seen[abs] = true
			out = append(out, abs)
		}
	})

	return out
}

func buildStructuredExtractionText(htmlBody string) string {
	doc, err := goquery.NewDocumentFromReader(strings.NewReader(htmlBody))
	if err != nil {
		return HTMLToText(htmlBody)
	}

	parts := make([]string, 0, 64)
	bodyText := cleanText(doc.Find("body").Text())
	if bodyText != "" {
		parts = append(parts, bodyText)
	}

	doc.Find("table tr").Each(func(_ int, row *goquery.Selection) {
		cells := make([]string, 0, 4)
		row.Find("th, td").Each(func(_ int, cell *goquery.Selection) {
			value := cleanText(cell.Text())
			if value != "" {
				cells = append(cells, value)
			}
		})
		if len(cells) == 0 {
			return
		}
		if len(cells) == 1 {
			parts = append(parts, cells[0])
			return
		}
		parts = append(parts, cells[0]+": "+strings.Join(cells[1:], " | "))
	})

	labelKeywords := []string{"cierre", "postul", "deadline", "closing", "submission", "fecha límite", "fecha maxima", "cronograma", "calendario", "opening", "apertura"}
	doc.Find("p, li, div, td, th, h1, h2, h3, h4, h5, h6, strong").Each(func(_ int, sel *goquery.Selection) {
		text := cleanText(sel.Text())
		if text == "" || len(text) > 220 {
			return
		}
		lower := strings.ToLower(text)
		for _, keyword := range labelKeywords {
			if strings.Contains(lower, keyword) {
				nextText := cleanText(sel.Next().Text())
				if nextText != "" && nextText != text {
					parts = append(parts, text+" | "+nextText)
				} else {
					parts = append(parts, text)
				}
				break
			}
		}
	})

	return strings.Join(parts, "\n")
}
