package ingest

import (
	"context"
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"log"
	"net/url"
	"sort"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/PuerkitoBio/goquery"
	"github.com/gocolly/colly/v2"
)

type HtmlGenericStrategy struct {
	// UseColly enables Colly-based scraping instead of the legacy goquery approach.
	UseColly bool
}

func (s *HtmlGenericStrategy) Run(ctx context.Context, config SourceConfig, p *Pipeline) (IngestionStats, error) {
	// Use Colly-based scraping by default
	if s.UseColly || true { // Always use Colly now
		return s.runWithColly(ctx, config, p)
	}
	return s.runLegacy(ctx, config, p)
}

// runWithColly uses Colly for web scraping with better rate limiting and error handling.
func (s *HtmlGenericStrategy) runWithColly(ctx context.Context, config SourceConfig, p *Pipeline) (IngestionStats, error) {
	stats := IngestionStats{}

	maxPages := config.MaxPages
	if maxPages == 0 {
		maxPages = 1
	}

	// Parse base URL to get domain
	parsedURL, err := url.Parse(config.BaseURL)
	if err != nil {
		return stats, fmt.Errorf("invalid base URL: %w", err)
	}

	// Configure Colly scraper
	scraperConfig := CollyScraperConfig{
		AllowedDomains:  []string{parsedURL.Host},
		MaxPages:        maxPages * 100, // Allow for detail pages
		DomainDelay:     1 * time.Second,
		ParallelThreads: 1, // Sequential for politeness
		UserAgent:       "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		RequestTimeout:  30 * time.Second,
	}

	// Apply source-specific fetch config
	if config.Fetch.TimeoutSeconds > 0 {
		scraperConfig.RequestTimeout = time.Duration(config.Fetch.TimeoutSeconds) * time.Second
	}
	if config.Fetch.RateLimitRPS > 0 {
		scraperConfig.DomainDelay = time.Duration(float64(time.Second) / config.Fetch.RateLimitRPS)
	}

	// Create main collector for list pages
	collector := colly.NewCollector(
		colly.AllowedDomains(parsedURL.Host),
		colly.UserAgent(scraperConfig.UserAgent),
		colly.DetectCharset(),
	)

	// Rate limiting
	collector.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: 1,
		Delay:       scraperConfig.DomainDelay,
		RandomDelay: scraperConfig.DomainDelay / 2,
	})

	collector.SetRequestTimeout(scraperConfig.RequestTimeout)

	// Detail collector (cloned with same settings)
	detailCollector := collector.Clone()

	visitedURLs := make(map[string]bool)
	pageCount := 0
	var nextPageURL string

	sel := config.Selectors
	if sel.Container == "" {
		return stats, fmt.Errorf("selector 'container' is required for html_generic strategy")
	}

	// Process items on list pages
	collector.OnHTML(sel.Container, func(e *colly.HTMLElement) {
		title := strings.TrimSpace(e.ChildText(config.Selectors.Title))

		// Link extraction
		linkAttr := config.Selectors.LinkAttr
		if linkAttr == "" {
			linkAttr = "href"
		}

		var link string
		if config.Selectors.Link == "" || config.Selectors.Link == "." {
			link = strings.TrimSpace(e.Attr(linkAttr))
		} else {
			link = strings.TrimSpace(e.ChildAttr(config.Selectors.Link, linkAttr))
		}

		summary := ""
		if config.Selectors.Content != "" {
			summary = strings.TrimSpace(e.ChildText(config.Selectors.Content))
		}

		if title == "" || link == "" {
			return
		}

		// Resolve relative URL
		fullURL := e.Request.AbsoluteURL(link)
		canonicalURL := CanonicalizeURL(fullURL)

		// Generate stable SourceID
		hash := sha1.Sum([]byte(canonicalURL))
		sourceID := hex.EncodeToString(hash[:])

		raw := RawOpportunity{
			Title:        title,
			ExternalURL:  canonicalURL,
			SourceDomain: extractDomain(config.BaseURL),
			SourceID:     sourceID,
			Description:  summary,
			Extra:        make(map[string]string),
		}

		// Pass config to Extra for normalization context
		if len(config.Detail.Parse.DateLocales) > 0 {
			raw.Extra["date_locales"] = strings.Join(config.Detail.Parse.DateLocales, ",")
		}

		stats.TotalFound++

		// Detail Enrichment with Colly
		if config.Detail.Enabled {
			if err := s.enrichOpportunityColly(ctx, &raw, config.Detail, detailCollector); err != nil {
				log.Printf("[%s] Detail fetch failed for %s: %v", config.ID, raw.ExternalURL, err)
			}
		}

		if err := p.SaveRaw(ctx, raw); err != nil {
			log.Printf("[%s] Failed to save %q: %v", config.ID, title, err)
			stats.Errors++
		} else {
			stats.TotalSaved++
		}
	})

	// Handle pagination
	if config.Pagination.Next != "" {
		collector.OnHTML(config.Pagination.Next, func(e *colly.HTMLElement) {
			nextPageURL = e.Request.AbsoluteURL(e.Attr("href"))
		})
	}

	collector.OnRequest(func(r *colly.Request) {
		log.Printf("[%s] Visiting: %s", config.ID, r.URL.String())
	})

	collector.OnError(func(r *colly.Response, err error) {
		log.Printf("[%s] Error fetching %s: %v", config.ID, r.Request.URL, err)
		stats.Errors++
	})

	// Scrape pages
	currentURL := config.BaseURL

	for pageCount < maxPages {
		canonPage := CanonicalizeURL(currentURL)
		if visitedURLs[canonPage] {
			log.Printf("[%s] Pagination cycle detected at %s. Stopping.", config.ID, canonPage)
			break
		}
		visitedURLs[canonPage] = true
		pageCount++

		log.Printf("[%s] Fetching page %d: %s", config.ID, pageCount, currentURL)
		nextPageURL = "" // Reset

		if err := collector.Visit(currentURL); err != nil {
			log.Printf("[%s] Fetch error on page %d: %v", config.ID, pageCount, err)
			break
		}

		collector.Wait()

		if nextPageURL == "" || config.Pagination.Next == "" {
			break
		}
		currentURL = nextPageURL
	}

	return stats, nil
}

// enrichOpportunityColly fetches detail page using Colly collector.
func (s *HtmlGenericStrategy) enrichOpportunityColly(ctx context.Context, raw *RawOpportunity, config DetailConfig, c *colly.Collector) error {
	log.Printf("Fetching details for: %s", raw.ExternalURL)

	var enrichErr error
	enriched := false

	clone := c.Clone()
	clone.OnResponse(func(r *colly.Response) {
		doc, err := goquery.NewDocumentFromReader(strings.NewReader(string(r.Body)))
		if err != nil {
			enrichErr = err
			return
		}

		s.extractDetailContent(raw, config, doc)
		enriched = true
	})

	clone.OnError(func(r *colly.Response, err error) {
		enrichErr = err
	})

	if err := clone.Visit(raw.ExternalURL); err != nil {
		return err
	}

	clone.Wait()

	if enrichErr != nil {
		return enrichErr
	}

	if !enriched {
		return fmt.Errorf("no response received for detail page")
	}

	return nil
}

// extractDetailContent extracts metadata from a detail page document.
func (s *HtmlGenericStrategy) extractDetailContent(raw *RawOpportunity, config DetailConfig, htmlDoc *goquery.Document) {
	sel := config.Selectors
	container := htmlDoc.Selection
	if sel.Container != "" {
		container = htmlDoc.Find(sel.Container)
	}

	// 1. Description (HTML or Text)
	if sel.Description != "" {
		htmlContent, _ := container.Find(sel.Description).Html()
		desc := strings.TrimSpace(htmlContent)
		if desc != "" {
			if !utf8.ValidString(desc) {
				desc = strings.ToValidUTF8(desc, "")
			}
			raw.Description = desc
		}
	}

	if strings.TrimSpace(raw.Description) == "" {
		if htmlContent, err := container.Html(); err == nil {
			raw.Description = strings.TrimSpace(htmlContent)
		}
	}

	// 2. Deadline
	if sel.Deadline != "" {
		deadlineSel := container.Find(sel.Deadline)
		deadlineText := strings.TrimSpace(deadlineSel.Text())
		if deadlineText != "" {
			raw.RawDeadline = deadlineText
		}
	}

	// 3. Amount
	if sel.Amount != "" {
		amountSel := container.Find(sel.Amount)
		amountText := strings.TrimSpace(amountSel.Text())
		if amountText != "" {
			raw.RawAmount = amountText
			if config.Parse.CurrencyDefault != "" {
				raw.RawCurrency = config.Parse.CurrencyDefault
			}
		}
	}

	structuredText := buildStructuredExtractionText(raw.Description)
	if strings.TrimSpace(structuredText) == "" {
		structuredText = buildStructuredExtractionText(htmlDoc.Selection.Text())
	}

	// 4. Detect rolling status from text
	containerText := strings.ToLower(structuredText)
	rollingKeywords := []string{
		"rolling basis", "rolling deadline", "open until filled",
		"ventanilla abierta", "convocatoria permanente", "postula todo el año", "continuously",
		"ongoing", "open-ended", "no deadline",
	}
	for _, keyword := range rollingKeywords {
		if strings.Contains(containerText, keyword) {
			raw.Extra["is_rolling"] = "true"
			break
		}
	}

	deadlineEvidence := parseDeadlineEvidenceFromText(strings.ToLower(structuredText), "detail_html", raw.ExternalURL, 0.82)
	if len(deadlineEvidence) > 0 {
		raw.DeadlineEvidence = append(raw.DeadlineEvidence, deadlineEvidence...)
		for _, ev := range deadlineEvidence {
			raw.DeadlineCandidates = appendUnique(raw.DeadlineCandidates, ev.ParsedDateISO)
		}

		sort.Slice(raw.DeadlineEvidence, func(i, j int) bool {
			ti, _ := time.Parse(time.RFC3339, raw.DeadlineEvidence[i].ParsedDateISO)
			tj, _ := time.Parse(time.RFC3339, raw.DeadlineEvidence[j].ParsedDateISO)
			return ti.Before(tj)
		})

		if bestClose := pickPreferredCloseEvidence(raw.DeadlineEvidence); bestClose != nil {
			raw.CloseISO = bestClose.ParsedDateISO
			raw.RawDeadline = bestClose.Snippet
		} else if len(raw.DeadlineEvidence) > 0 {
			raw.CloseISO = raw.DeadlineEvidence[len(raw.DeadlineEvidence)-1].ParsedDateISO
			raw.RawDeadline = raw.DeadlineEvidence[len(raw.DeadlineEvidence)-1].Snippet
		}
		if raw.OpenISO == "" {
			for _, ev := range raw.DeadlineEvidence {
				label := strings.ToLower(ev.Label + " " + ev.Snippet)
				if strings.Contains(label, "inicio") || strings.Contains(label, "opening") || strings.Contains(label, "apertura") {
					raw.OpenISO = ev.ParsedDateISO
					break
				}
			}
		}
	}

	// 5. Detect status from text
	statusText := strings.ToLower(containerText)
	if strings.Contains(statusText, "closed") || strings.Contains(statusText, "cerrada") ||
		strings.Contains(statusText, "results") || strings.Contains(statusText, "awarded") ||
		strings.Contains(statusText, "finalizada") {
		raw.Extra["opp_status"] = "closed"
		raw.Extra["source_status_raw"] = "closed"
	} else if strings.Contains(statusText, "forthcoming") || strings.Contains(statusText, "upcoming") ||
		strings.Contains(statusText, "próximamente") || strings.Contains(statusText, "coming soon") {
		raw.Extra["opp_status"] = "forthcoming"
		raw.Extra["source_status_raw"] = "forthcoming"
	}

	if strings.Contains(statusText, "resultados finales") || strings.Contains(statusText, "ganadores") ||
		strings.Contains(statusText, "winners") || strings.Contains(statusText, "awardees") ||
		strings.Contains(statusText, "ranking") {
		raw.IsResultsPage = true
		raw.Extra["is_results_page"] = "true"
	}

	// 6. Extract eligibility if selector exists
	if sel.Eligibility != "" {
		eligibilitySel := container.Find(sel.Eligibility)
		eligibilityText := strings.TrimSpace(eligibilitySel.Text())
		if eligibilityText != "" {
			raw.Extra["eligibility"] = eligibilityText
		}
	}
}

// runLegacy uses the original goquery-based approach (kept for fallback).
func (s *HtmlGenericStrategy) runLegacy(ctx context.Context, config SourceConfig, p *Pipeline) (IngestionStats, error) {
	stats := IngestionStats{}

	// Default MaxPages if not set
	maxPages := config.MaxPages
	if maxPages == 0 {
		maxPages = 1
	}

	currentURL := config.BaseURL
	pageCount := 0

	visitedURLs := make(map[string]bool)

	for pageCount < maxPages {
		// Pagination Cycle Detection - canonicalize URL before comparing
		canonPage := CanonicalizeURL(currentURL)
		if visitedURLs[canonPage] {
			log.Printf("[%s] Pagination cycle detected at %s. Stopping.", config.ID, canonPage)
			break
		}
		visitedURLs[canonPage] = true

		pageCount++
		log.Printf("[%s] Fetching page %d: %s", config.ID, pageCount, currentURL)

		fetchedDoc, err := p.Fetcher.Fetch(ctx, currentURL)
		if err != nil {
			log.Printf("[%s] Fetch error on page %d: %v", config.ID, pageCount, err)
			break
		}

		// Create goquery doc
		doc, err := goquery.NewDocumentFromReader(fetchedDoc.Body)
		fetchedDoc.Body.Close() // Close immediately

		if err != nil {
			log.Printf("[%s] Parse error on page %d: %v", config.ID, pageCount, err)
			break
		}

		// 3. Extract items
		sel := config.Selectors
		if sel.Container == "" {
			return stats, fmt.Errorf("selector 'container' is required for html_generic strategy")
		}

		container := doc.Find(sel.Container)
		itemCount := container.Length()
		stats.TotalFound += itemCount
		log.Printf("[%s] Page %d: Found %d items", config.ID, pageCount, itemCount)

		container.Each(func(i int, sel *goquery.Selection) {
			title := strings.TrimSpace(sel.Find(config.Selectors.Title).Text())

			// Link extraction logic
			linkAttr := config.Selectors.LinkAttr
			if linkAttr == "" {
				linkAttr = "href"
			}

			var link string
			if config.Selectors.Link == "" || config.Selectors.Link == "." {
				link = strings.TrimSpace(sel.AttrOr(linkAttr, ""))
			} else {
				link = strings.TrimSpace(sel.Find(config.Selectors.Link).AttrOr(linkAttr, ""))
			}

			summary := ""
			if config.Selectors.Content != "" {
				summary = strings.TrimSpace(sel.Find(config.Selectors.Content).Text())
			}

			if title == "" || link == "" {
				return
			}

			// Resolve relative URL
			fullURL := link
			if !strings.HasPrefix(link, "http") {
				u, err := url.Parse(currentURL) // Resolve against current page URL
				if err == nil {
					rel, err := url.Parse(link)
					if err == nil {
						fullURL = u.ResolveReference(rel).String()
					}
				}
			}

			// Canonicalize URL and generate stable SourceID
			canonicalURL := CanonicalizeURL(fullURL)
			// hash := sha1.Sum([]byte(canonicalURL))
			// sourceID := hex.EncodeToString(hash[:])
			// SourceID is generated in FromRaw or Pipeline if empty, but we can generate it here for consistency
			// Actually, FromRaw expects SourceID.
			hash := sha1.Sum([]byte(canonicalURL))
			sourceID := hex.EncodeToString(hash[:])

			raw := RawOpportunity{
				Title:        title,
				ExternalURL:  canonicalURL, // normalized but still "raw" input to pipeline
				SourceDomain: extractDomain(config.BaseURL),
				SourceID:     sourceID,
				Description:  summary,
				Extra:        make(map[string]string),
			}

			// Pass config to Extra for normalization context
			if len(config.Detail.Parse.DateLocales) > 0 {
				raw.Extra["date_locales"] = strings.Join(config.Detail.Parse.DateLocales, ",")
			}

			// Detail Enrichment
			if config.Detail.Enabled {
				// Be polite between detail fetches
				time.Sleep(500 * time.Millisecond)
				if err := s.enrichOpportunity(ctx, &raw, config.Detail, p); err != nil {
					log.Printf("[%s] Detail fetch failed for %s: %v", config.ID, raw.ExternalURL, err)
				}
			}

			if err := p.SaveRaw(ctx, raw); err != nil {
				log.Printf("[%s] Failed to save %q: %v", config.ID, title, err)
				stats.Errors++
			} else {
				stats.TotalSaved++
			}
		})

		// 4. Pagination
		if config.Pagination.Next != "" {
			nextLink := doc.Find(config.Pagination.Next).AttrOr("href", "")
			if nextLink == "" {
				log.Printf("[%s] No next link found on page %d", config.ID, pageCount)
				break
			}

			// Resolve next link
			if !strings.HasPrefix(nextLink, "http") {
				u, err := url.Parse(currentURL)
				if err == nil {
					rel, err := url.Parse(nextLink)
					if err == nil {
						nextLink = u.ResolveReference(rel).String()
					}
				}
			}
			currentURL = nextLink
		} else {
			break
		}
	}

	return stats, nil
}

// CanonicalizeURL removes common tracking parameters to ensure stable URLs.
func CanonicalizeURL(rawURL string) string {
	u, err := url.Parse(rawURL)
	if err != nil {
		return rawURL
	}

	// Normalize host to lowercase
	u.Host = strings.ToLower(u.Host)
	// Remove fragment
	u.Fragment = ""

	q := u.Query()
	// Remove common tracking params
	paramsToRemovePrefix := []string{"utm_"}
	exactParamsToRemove := []string{
		"fbclid", "gclid", "mc_cid", "mc_eid", "mkt_tok", "ref", "session", "s_cid",
	}

	for k := range q {
		for _, prefix := range paramsToRemovePrefix {
			if strings.HasPrefix(k, prefix) {
				q.Del(k)
			}
		}
	}

	for _, p := range exactParamsToRemove {
		q.Del(p)
	}

	u.RawQuery = q.Encode()
	return u.String()
}

// enrichOpportunity fetches the detail page and extracts additional metadata.
func (s *HtmlGenericStrategy) enrichOpportunity(ctx context.Context, raw *RawOpportunity, config DetailConfig, p *Pipeline) error {
	log.Printf("Fetching details for: %s", raw.ExternalURL)
	doc, err := p.Fetcher.Fetch(ctx, raw.ExternalURL)
	if err != nil {
		return err
	}
	defer doc.Body.Close()

	htmlDoc, err := goquery.NewDocumentFromReader(doc.Body)
	if err != nil {
		return err
	}

	sel := config.Selectors
	container := htmlDoc.Selection
	if sel.Container != "" {
		container = htmlDoc.Find(sel.Container)
	}

	// 1. Description (HTML or Text)
	if sel.Description != "" {
		htmlContent, _ := container.Find(sel.Description).Html()
		desc := strings.TrimSpace(htmlContent)
		if desc != "" {
			// Sanitize UTF-8
			if !utf8.ValidString(desc) {
				desc = strings.ToValidUTF8(desc, "")
			}
			raw.Description = desc
		}
	}

	if strings.TrimSpace(raw.Description) == "" {
		if htmlContent, err := container.Html(); err == nil {
			raw.Description = strings.TrimSpace(htmlContent)
		}
	}

	// 2. Deadline
	if sel.Deadline != "" {
		deadlineSel := container.Find(sel.Deadline)
		deadlineText := strings.TrimSpace(deadlineSel.Text())
		if deadlineText != "" {
			raw.RawDeadline = deadlineText
		}
	}

	// 3. Amount
	if sel.Amount != "" {
		amountSel := container.Find(sel.Amount)
		amountText := strings.TrimSpace(amountSel.Text())
		if amountText != "" {
			raw.RawAmount = amountText
			if config.Parse.CurrencyDefault != "" {
				raw.RawCurrency = config.Parse.CurrencyDefault
			}
		}
	}

	structuredText := buildStructuredExtractionText(raw.Description)
	if strings.TrimSpace(structuredText) == "" {
		structuredText = buildStructuredExtractionText(htmlDoc.Selection.Text())
	}

	// 4. Detect rolling status from text
	containerText := strings.ToLower(structuredText)
	rollingKeywords := []string{
		"rolling basis", "rolling deadline", "open until filled",
		"ventanilla abierta", "convocatoria permanente", "postula todo el año", "continuously",
		"ongoing", "open-ended", "no deadline",
	}
	for _, keyword := range rollingKeywords {
		if strings.Contains(containerText, keyword) {
			raw.Extra["is_rolling"] = "true"
			break
		}
	}

	deadlineEvidence := parseDeadlineEvidenceFromText(strings.ToLower(structuredText), "detail_html", raw.ExternalURL, 0.82)
	if len(deadlineEvidence) > 0 {
		raw.DeadlineEvidence = append(raw.DeadlineEvidence, deadlineEvidence...)
		for _, ev := range deadlineEvidence {
			raw.DeadlineCandidates = appendUnique(raw.DeadlineCandidates, ev.ParsedDateISO)
		}

		sort.Slice(raw.DeadlineEvidence, func(i, j int) bool {
			ti, _ := time.Parse(time.RFC3339, raw.DeadlineEvidence[i].ParsedDateISO)
			tj, _ := time.Parse(time.RFC3339, raw.DeadlineEvidence[j].ParsedDateISO)
			return ti.Before(tj)
		})

		if bestClose := pickPreferredCloseEvidence(raw.DeadlineEvidence); bestClose != nil {
			raw.CloseISO = bestClose.ParsedDateISO
			raw.RawDeadline = bestClose.Snippet
		} else if len(raw.DeadlineEvidence) > 0 {
			raw.CloseISO = raw.DeadlineEvidence[len(raw.DeadlineEvidence)-1].ParsedDateISO
			raw.RawDeadline = raw.DeadlineEvidence[len(raw.DeadlineEvidence)-1].Snippet
		}
		if raw.OpenISO == "" {
			for _, ev := range raw.DeadlineEvidence {
				label := strings.ToLower(ev.Label + " " + ev.Snippet)
				if strings.Contains(label, "inicio") || strings.Contains(label, "opening") || strings.Contains(label, "apertura") {
					raw.OpenISO = ev.ParsedDateISO
					break
				}
			}
		}
	}

	// 5. Detect status from text
	statusText := strings.ToLower(containerText)
	if strings.Contains(statusText, "closed") || strings.Contains(statusText, "cerrada") ||
		strings.Contains(statusText, "results") || strings.Contains(statusText, "awarded") ||
		strings.Contains(statusText, "finalizada") {
		raw.Extra["opp_status"] = "closed"
		raw.Extra["source_status_raw"] = "closed"
	} else if strings.Contains(statusText, "forthcoming") || strings.Contains(statusText, "upcoming") ||
		strings.Contains(statusText, "próximamente") || strings.Contains(statusText, "coming soon") {
		raw.Extra["opp_status"] = "forthcoming"
		raw.Extra["source_status_raw"] = "forthcoming"
	}

	if strings.Contains(statusText, "resultados finales") || strings.Contains(statusText, "ganadores") ||
		strings.Contains(statusText, "winners") || strings.Contains(statusText, "awardees") ||
		strings.Contains(statusText, "ranking") {
		raw.IsResultsPage = true
		raw.Extra["is_results_page"] = "true"
	}

	// 6. Extract eligibility if selector exists
	if sel.Eligibility != "" {
		eligibilitySel := container.Find(sel.Eligibility)
		eligibilityText := strings.TrimSpace(eligibilitySel.Text())
		if eligibilityText != "" {
			raw.Extra["eligibility"] = eligibilityText
		}
	}

	return nil
}

// Legacy parseDate function kept for backward compatibility
// Now uses the robust parser
func parseDate(s string) (time.Time, error) {
	return parseDateRobust(s, []string{"en"})
}

func pickPreferredCloseEvidence(evidence []DeadlineEvidence) *DeadlineEvidence {
	if len(evidence) == 0 {
		return nil
	}
	now := time.Now().UTC()
	closeHints := []string{"cierre", "deadline", "postul", "submission", "closes", "fecha máxima", "fecha limite"}

	var preferred *DeadlineEvidence
	for i := range evidence {
		ev := evidence[i]
		label := strings.ToLower(ev.Label + " " + ev.Snippet)
		matched := false
		for _, hint := range closeHints {
			if strings.Contains(label, hint) {
				matched = true
				break
			}
		}
		if !matched {
			continue
		}
		ts, err := time.Parse(time.RFC3339, ev.ParsedDateISO)
		if err != nil || !ts.After(now) {
			continue
		}
		if preferred == nil {
			preferred = &evidence[i]
			continue
		}
		prevTs, _ := time.Parse(time.RFC3339, preferred.ParsedDateISO)
		if ts.Before(prevTs) {
			preferred = &evidence[i]
		}
	}

	if preferred != nil {
		return preferred
	}

	for i := range evidence {
		ts, err := time.Parse(time.RFC3339, evidence[i].ParsedDateISO)
		if err == nil && ts.After(now) {
			return &evidence[i]
		}
	}

	return &evidence[len(evidence)-1]
}
