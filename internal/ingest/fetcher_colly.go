package ingest

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gocolly/colly/v2"
)

// CollyFetcher implements Fetcher interface using Colly for web scraping.
// It provides rate limiting, retries, and respects robots.txt.
type CollyFetcher struct {
	UserAgent           string
	MaxRetries          int
	RequestTimeout      time.Duration
	DomainDelay         time.Duration
	RandomDelayFactor   float64
	AllowedDomains      []string
	IgnoreRobotsTxt     bool
	MaxBodySize         int // bytes, 0 = unlimited
	DetectCharset       bool
	CacheDir            string // empty = no cache
	AllowURLRevisit     bool
	Async               bool
	ParallelThreads     int
}

// NewCollyFetcher creates a CollyFetcher with sensible defaults.
func NewCollyFetcher() *CollyFetcher {
	return &CollyFetcher{
		UserAgent:         "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
		MaxRetries:        3,
		RequestTimeout:    30 * time.Second,
		DomainDelay:       1 * time.Second,
		RandomDelayFactor: 0.5,
		IgnoreRobotsTxt:   false,
		MaxBodySize:       10 * 1024 * 1024, // 10MB
		DetectCharset:     true,
		AllowURLRevisit:   true,
		Async:             false,
		ParallelThreads:   2,
	}
}

// buildCollector creates a configured Colly collector.
func (f *CollyFetcher) buildCollector(allowedDomains []string) *colly.Collector {
	opts := []colly.CollectorOption{
		colly.UserAgent(f.UserAgent),
		colly.MaxBodySize(f.MaxBodySize),
		colly.AllowURLRevisit(),
	}

	if len(allowedDomains) > 0 {
		opts = append(opts, colly.AllowedDomains(allowedDomains...))
	}

	if f.DetectCharset {
		opts = append(opts, colly.DetectCharset())
	}

	if f.IgnoreRobotsTxt {
		opts = append(opts, colly.IgnoreRobotsTxt())
	}

	if f.CacheDir != "" {
		opts = append(opts, colly.CacheDir(f.CacheDir))
	}

	if f.Async {
		opts = append(opts, colly.Async(true))
	}

	c := colly.NewCollector(opts...)

	// Configure rate limiting
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: f.ParallelThreads,
		Delay:       f.DomainDelay,
		RandomDelay: time.Duration(float64(f.DomainDelay) * f.RandomDelayFactor),
	})

	c.SetRequestTimeout(f.RequestTimeout)

	// Retry on errors
	c.OnError(func(r *colly.Response, err error) {
		if r.Request.Ctx.GetAny("retries") == nil {
			r.Request.Ctx.Put("retries", 0)
		}
		retries := r.Request.Ctx.GetAny("retries").(int)
		if retries < f.MaxRetries {
			r.Request.Ctx.Put("retries", retries+1)
			log.Printf("[Colly] Retry %d/%d for %s: %v", retries+1, f.MaxRetries, r.Request.URL, err)
			time.Sleep(time.Duration(retries+1) * time.Second)
			r.Request.Retry()
		}
	})

	return c
}

// Fetch implements the Fetcher interface, returning a FetchedDocument.
func (f *CollyFetcher) Fetch(ctx context.Context, targetURL string) (*FetchedDocument, error) {
	parsedURL, err := url.Parse(targetURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Extract domain for allowed domains config
	domains := []string{parsedURL.Host}
	c := f.buildCollector(domains)

	var result *FetchedDocument
	var fetchErr error
	var wg sync.WaitGroup
	wg.Add(1)

	c.OnResponse(func(r *colly.Response) {
		defer wg.Done()
		result = &FetchedDocument{
			URL:         r.Request.URL.String(),
			StatusCode:  r.StatusCode,
			ContentType: r.Headers.Get("Content-Type"),
			Body:        io.NopCloser(bytes.NewReader(r.Body)),
			FetchedAt:   time.Now(),
			Headers:     map[string][]string(r.Headers.Clone()),
		}
	})

	c.OnError(func(r *colly.Response, err error) {
		retries := 0
		if r.Request.Ctx.GetAny("retries") != nil {
			retries = r.Request.Ctx.GetAny("retries").(int)
		}
		if retries >= f.MaxRetries {
			fetchErr = fmt.Errorf("fetch failed after %d retries: %w", f.MaxRetries, err)
			wg.Done()
		}
	})

	// Handle context cancellation
	done := make(chan struct{})
	go func() {
		select {
		case <-ctx.Done():
			fetchErr = ctx.Err()
			wg.Done()
		case <-done:
		}
	}()

	if err := c.Visit(targetURL); err != nil {
		close(done)
		return nil, fmt.Errorf("visit failed: %w", err)
	}

	wg.Wait()
	close(done)

	if fetchErr != nil {
		return nil, fetchErr
	}

	if result == nil {
		return nil, fmt.Errorf("no response received for %s", targetURL)
	}

	return result, nil
}

// CollyScraperConfig provides configuration for advanced Colly scraping.
type CollyScraperConfig struct {
	AllowedDomains    []string
	MaxDepth          int
	MaxPages          int
	DomainDelay       time.Duration
	ParallelThreads   int
	IgnoreRobotsTxt   bool
	UserAgent         string
	RequestTimeout    time.Duration
	CacheDir          string
	Headers           map[string]string
	ProxyURL          string
}

// CollyScraper provides advanced Colly-based scraping with callbacks.
type CollyScraper struct {
	config    CollyScraperConfig
	collector *colly.Collector
	visited   map[string]bool
	mu        sync.RWMutex
}

// NewCollyScraper creates a new scraper with the given configuration.
func NewCollyScraper(config CollyScraperConfig) *CollyScraper {
	// Set defaults
	if config.UserAgent == "" {
		config.UserAgent = "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
	}
	if config.RequestTimeout == 0 {
		config.RequestTimeout = 30 * time.Second
	}
	if config.DomainDelay == 0 {
		config.DomainDelay = 1 * time.Second
	}
	if config.ParallelThreads == 0 {
		config.ParallelThreads = 2
	}
	if config.MaxDepth == 0 {
		config.MaxDepth = 2
	}
	if config.MaxPages == 0 {
		config.MaxPages = 100
	}

	opts := []colly.CollectorOption{
		colly.UserAgent(config.UserAgent),
		colly.MaxDepth(config.MaxDepth),
		colly.DetectCharset(),
	}

	if len(config.AllowedDomains) > 0 {
		opts = append(opts, colly.AllowedDomains(config.AllowedDomains...))
	}

	if config.IgnoreRobotsTxt {
		opts = append(opts, colly.IgnoreRobotsTxt())
	}

	if config.CacheDir != "" {
		opts = append(opts, colly.CacheDir(config.CacheDir))
	}

	c := colly.NewCollector(opts...)

	// Rate limiting
	c.Limit(&colly.LimitRule{
		DomainGlob:  "*",
		Parallelism: config.ParallelThreads,
		Delay:       config.DomainDelay,
		RandomDelay: config.DomainDelay / 2,
	})

	c.SetRequestTimeout(config.RequestTimeout)

	// Custom headers
	if len(config.Headers) > 0 {
		c.OnRequest(func(r *colly.Request) {
			for k, v := range config.Headers {
				r.Headers.Set(k, v)
			}
		})
	}

	// Proxy
	if config.ProxyURL != "" {
		c.SetProxy(config.ProxyURL)
	}

	return &CollyScraper{
		config:    config,
		collector: c,
		visited:   make(map[string]bool),
	}
}

// Collector returns the underlying Colly collector for custom callbacks.
func (s *CollyScraper) Collector() *colly.Collector {
	return s.collector
}

// ScrapeResult represents a scraped page.
type ScrapeResult struct {
	URL         string
	Title       string
	Body        []byte
	HTML        string
	StatusCode  int
	ContentType string
	Links       []string
	ScrapedAt   time.Time
}

// OnHTML registers a callback for HTML elements matching the selector.
func (s *CollyScraper) OnHTML(selector string, callback func(e *colly.HTMLElement)) {
	s.collector.OnHTML(selector, callback)
}

// OnResponse registers a callback for responses.
func (s *CollyScraper) OnResponse(callback func(r *colly.Response)) {
	s.collector.OnResponse(callback)
}

// OnError registers a callback for errors.
func (s *CollyScraper) OnError(callback func(r *colly.Response, err error)) {
	s.collector.OnError(callback)
}

// OnRequest registers a callback for requests.
func (s *CollyScraper) OnRequest(callback func(r *colly.Request)) {
	s.collector.OnRequest(callback)
}

// Visit starts scraping from the given URL.
func (s *CollyScraper) Visit(url string) error {
	s.mu.Lock()
	if s.visited[url] {
		s.mu.Unlock()
		return nil
	}
	if len(s.visited) >= s.config.MaxPages {
		s.mu.Unlock()
		return fmt.Errorf("max pages limit (%d) reached", s.config.MaxPages)
	}
	s.visited[url] = true
	s.mu.Unlock()

	return s.collector.Visit(url)
}

// Wait waits for all async scraping to complete.
func (s *CollyScraper) Wait() {
	s.collector.Wait()
}

// VisitedCount returns the number of visited URLs.
func (s *CollyScraper) VisitedCount() int {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return len(s.visited)
}

// Clone creates a child collector for detail page scraping.
func (s *CollyScraper) Clone() *colly.Collector {
	return s.collector.Clone()
}

// ListPageItem represents an item extracted from a list page.
type ListPageItem struct {
	Title       string
	Link        string
	Summary     string
	DateStr     string
	ExtraAttrs  map[string]string
}

// ScrapeListPage scrapes a list page and extracts items using selectors.
func (s *CollyScraper) ScrapeListPage(pageURL string, containerSel, linkSel, titleSel, summarySel string) ([]ListPageItem, error) {
	var items []ListPageItem
	var scrapeErr error

	s.collector.OnHTML(containerSel, func(e *colly.HTMLElement) {
		item := ListPageItem{
			ExtraAttrs: make(map[string]string),
		}

		if titleSel != "" {
			item.Title = strings.TrimSpace(e.ChildText(titleSel))
		} else {
			item.Title = strings.TrimSpace(e.Text)
		}

		if linkSel != "" && linkSel != "." {
			item.Link = e.ChildAttr(linkSel, "href")
		} else {
			item.Link = e.Attr("href")
		}

		if summarySel != "" {
			item.Summary = strings.TrimSpace(e.ChildText(summarySel))
		}

		// Resolve relative URL
		if item.Link != "" && !strings.HasPrefix(item.Link, "http") {
			item.Link = e.Request.AbsoluteURL(item.Link)
		}

		if item.Title != "" && item.Link != "" {
			items = append(items, item)
		}
	})

	s.collector.OnError(func(r *colly.Response, err error) {
		scrapeErr = err
	})

	if err := s.collector.Visit(pageURL); err != nil {
		return nil, err
	}

	s.collector.Wait()

	if scrapeErr != nil {
		return nil, scrapeErr
	}

	return items, nil
}

// FetchWithColly is a helper function for one-off fetches using Colly.
func FetchWithColly(ctx context.Context, targetURL string) (*FetchedDocument, error) {
	fetcher := NewCollyFetcher()
	return fetcher.Fetch(ctx, targetURL)
}

// CollyFetcherWithConfig creates a CollyFetcher from a FetchConfig.
func CollyFetcherWithConfig(cfg FetchConfig) *CollyFetcher {
	f := NewCollyFetcher()

	if cfg.TimeoutSeconds > 0 {
		f.RequestTimeout = time.Duration(cfg.TimeoutSeconds) * time.Second
	}

	if cfg.RateLimitRPS > 0 {
		f.DomainDelay = time.Duration(float64(time.Second) / cfg.RateLimitRPS)
	}

	if cfg.MaxRetries > 0 {
		f.MaxRetries = cfg.MaxRetries
	}

	return f
}

// CollyHTTPTransport returns an http.RoundTripper that uses Colly internally.
// This is useful for integrating Colly with existing HTTP clients.
type CollyHTTPTransport struct {
	Fetcher *CollyFetcher
}

// RoundTrip implements http.RoundTripper.
func (t *CollyHTTPTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	doc, err := t.Fetcher.Fetch(req.Context(), req.URL.String())
	if err != nil {
		return nil, err
	}

	return &http.Response{
		StatusCode: doc.StatusCode,
		Header:     http.Header(doc.Headers),
		Body:       doc.Body,
		Request:    req,
	}, nil
}
