package ingest

import (
	"context"
	"fmt"
	"math/rand"
	"net"
	"net/netip"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"
)

var blockedPrefixStrings = []string{
	"127.0.0.0/8",
	"10.0.0.0/8",
	"172.16.0.0/12",
	"192.168.0.0/16",
	"169.254.0.0/16",
	"::1/128",
	"fc00::/7",
	"fe80::/10",
}

var blockedPrefixes = func() []netip.Prefix {
	prefixes := make([]netip.Prefix, 0, len(blockedPrefixStrings))
	for _, s := range blockedPrefixStrings {
		if p, err := netip.ParsePrefix(s); err == nil {
			prefixes = append(prefixes, p)
		}
	}
	return prefixes
}()

type HTTPFetcher struct {
	Client *http.Client
}

func NewHTTPFetcher() *HTTPFetcher {
	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           safeDialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	return &HTTPFetcher{
		Client: &http.Client{
			Timeout:       30 * time.Second,
			Transport:     transport,
			CheckRedirect: safeCheckRedirect,
		},
	}
}

func (f *HTTPFetcher) Fetch(ctx context.Context, url string) (*FetchedDocument, error) {
	req, err := http.NewRequestWithContext(ctx, "GET", url, nil)
	if err != nil {
		return nil, fmt.Errorf("failed to create request: %w", err)
	}

	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
	req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
	req.Header.Set("Accept-Language", "en-US,en;q=0.5")
	req.Header.Set("Cache-Control", "no-cache")
	req.Header.Set("Upgrade-Insecure-Requests", "1")

	resp, err := f.Client.Do(req)
	if err != nil {
		return nil, fmt.Errorf("failed to execute request: %w", err)
	}

	if resp.StatusCode != http.StatusOK {
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	return &FetchedDocument{
		URL:         url,
		StatusCode:  resp.StatusCode,
		ContentType: resp.Header.Get("Content-Type"),
		Body:        resp.Body,
		FetchedAt:   time.Now(),
		Headers:     resp.Header,
	}, nil
}

// RateLimitedFetcher provides rate limiting, retries, and configurable timeouts per domain
type RateLimitedFetcher struct {
	clients       map[string]*http.Client // per domain
	limiters      map[string]*time.Ticker // per domain (simple ticker-based rate limiting)
	configs       map[string]FetchConfig  // per domain config
	defaultConfig FetchConfig
	mu            sync.RWMutex
}

// NewRateLimitedFetcher creates a new rate-limited fetcher with default config
func NewRateLimitedFetcher(defaultConfig FetchConfig) *RateLimitedFetcher {
	if defaultConfig.TimeoutSeconds == 0 {
		defaultConfig.TimeoutSeconds = 30
	}
	if defaultConfig.MaxRetries == 0 {
		defaultConfig.MaxRetries = 3
	}
	if defaultConfig.RateLimitRPS == 0 {
		defaultConfig.RateLimitRPS = 1.0
	}
	if defaultConfig.AcceptLanguage == "" {
		defaultConfig.AcceptLanguage = "en-US,en;q=0.5"
	}

	return &RateLimitedFetcher{
		clients:       make(map[string]*http.Client),
		limiters:      make(map[string]*time.Ticker),
		configs:       make(map[string]FetchConfig),
		defaultConfig: defaultConfig,
	}
}

// getDomain extracts the domain from a URL
func getDomain(rawURL string) (string, error) {
	u, err := url.Parse(rawURL)
	if err != nil {
		return "", err
	}
	return u.Host, nil
}

// getClient returns or creates an HTTP client for a domain
func (f *RateLimitedFetcher) getClient(domain string, config FetchConfig) *http.Client {
	f.mu.RLock()
	client, exists := f.clients[domain]
	f.mu.RUnlock()

	if exists {
		return client
	}

	f.mu.Lock()
	defer f.mu.Unlock()

	// Double-check after acquiring write lock
	if client, exists := f.clients[domain]; exists {
		return client
	}

	// Create new client with config
	timeout := time.Duration(config.TimeoutSeconds) * time.Second
	if timeout == 0 {
		timeout = 30 * time.Second
	}

	transport := &http.Transport{
		Proxy:                 http.ProxyFromEnvironment,
		DialContext:           safeDialContext,
		ForceAttemptHTTP2:     true,
		MaxIdleConns:          100,
		IdleConnTimeout:       90 * time.Second,
		TLSHandshakeTimeout:   10 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
	}

	if config.ProxyURL != "" {
		proxyURL, err := url.Parse(config.ProxyURL)
		if err == nil {
			transport.Proxy = http.ProxyURL(proxyURL)
		}
	}

	client = &http.Client{
		Timeout:       timeout,
		Transport:     transport,
		CheckRedirect: safeCheckRedirect,
	}

	f.clients[domain] = client

	// Create rate limiter (ticker-based, simple approach)
	interval := time.Duration(float64(time.Second) / config.RateLimitRPS)
	if interval == 0 {
		interval = time.Second
	}
	f.limiters[domain] = time.NewTicker(interval)
	f.configs[domain] = config

	return client
}

// safeDialContext wraps the default dialer to block private IPs
func safeDialContext(ctx context.Context, network, addr string) (net.Conn, error) {
	d := &net.Dialer{
		Timeout:   30 * time.Second,
		KeepAlive: 30 * time.Second,
	}

	// Split host and port
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return nil, err
	}

	// Resolve IPs
	ips, err := net.LookupIP(host)
	if err != nil {
		return nil, err
	}

	for _, ip := range ips {
		if isPrivateIP(ip) {
			return nil, fmt.Errorf("blocked private IP: %s", ip)
		}
	}

	// If safe, obtain connection.
	// Note: race condition between check and usage (TOCTOU) exists but is mitigated by typical DNS caching
	// and is standard mitigation unless using a custom control func in Go 1.20+.
	// Ideally we use Control in Dialer, but for now pre-resolution check is the standard "easy" fix.
	return d.DialContext(ctx, network, addr)
}

// isPrivateIP checks if an IP is in a private range or loopback/link-local
func isPrivateIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsLinkLocalMulticast() || ip.IsLinkLocalUnicast() || ip.IsMulticast() || ip.IsPrivate() || ip.IsUnspecified() {
		return true
	}

	addr, ok := netip.AddrFromSlice(ip)
	if ok {
		for _, prefix := range blockedPrefixes {
			if prefix.Contains(addr.Unmap()) {
				return true
			}
		}
	}

	if ip4 := ip.To4(); ip4 != nil {
		switch {
		case ip4[0] == 10:
			return true
		case ip4[0] == 172 && ip4[1] >= 16 && ip4[1] <= 31:
			return true
		case ip4[0] == 192 && ip4[1] == 168:
			return true
		case ip4[0] == 169 && ip4[1] == 254:
			return true
		}
		return false
	}

	return false // Allow IPv6 global unicast, but ideally check fc00::/7 etc.
}

// safeCheckRedirect limits redirects and validates destinations
func safeCheckRedirect(req *http.Request, via []*http.Request) error {
	if len(via) >= 10 {
		return fmt.Errorf("stopped after 10 redirects")
	}
	if req.URL == nil {
		return fmt.Errorf("invalid redirect URL")
	}
	if req.URL.Scheme != "http" && req.URL.Scheme != "https" {
		return fmt.Errorf("redirect scheme blocked")
	}

	// Validate redirect target IP
	host := req.URL.Hostname()
	if host == "" {
		return fmt.Errorf("redirect host missing")
	}
	if strings.EqualFold(host, "localhost") || strings.HasSuffix(strings.ToLower(host), ".local") {
		return fmt.Errorf("redirect to internal host blocked")
	}
	ips, err := net.LookupIP(host)
	if err != nil {
		return err
	}
	if len(ips) == 0 {
		return fmt.Errorf("redirect host resolved to no addresses")
	}
	for _, ip := range ips {
		if isPrivateIP(ip) {
			return fmt.Errorf("redirect to private IP blocked: %s", ip)
		}
	}

	return nil
}

// shouldRetry determines if an error or status code should trigger a retry
func shouldRetry(err error, statusCode int) bool {
	if err != nil {
		// Check for timeout errors
		if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
			return true
		}
		return false
	}

	// Retry on these status codes
	retryStatusCodes := map[int]bool{
		429: true, // Too Many Requests
		500: true, // Internal Server Error
		502: true, // Bad Gateway
		503: true, // Service Unavailable
		504: true, // Gateway Timeout
	}
	return retryStatusCodes[statusCode]
}

// Fetch implements the Fetcher interface with rate limiting and retries
func (f *RateLimitedFetcher) Fetch(ctx context.Context, rawURL string) (*FetchedDocument, error) {
	domain, err := getDomain(rawURL)
	if err != nil {
		return nil, fmt.Errorf("invalid URL: %w", err)
	}

	// Get config for this domain or use default
	config := f.defaultConfig
	f.mu.RLock()
	if domainConfig, exists := f.configs[domain]; exists {
		config = domainConfig
	}
	f.mu.RUnlock()

	// Get client for this domain
	client := f.getClient(domain, config)

	// Wait for rate limiter
	f.mu.RLock()
	limiter, exists := f.limiters[domain]
	f.mu.RUnlock()
	if exists {
		<-limiter.C
	}

	// Retry logic with exponential backoff
	var lastErr error
	var lastResp *http.Response

	for attempt := 0; attempt <= config.MaxRetries; attempt++ {
		if attempt > 0 {
			// Exponential backoff: 0.5s, 1s, 2s + jitter
			backoff := time.Duration(500*(1<<uint(attempt-1))) * time.Millisecond
			jitter := time.Duration(rand.Intn(100)) * time.Millisecond
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-time.After(backoff + jitter):
			}
		}

		req, err := http.NewRequestWithContext(ctx, "GET", rawURL, nil)
		if err != nil {
			return nil, fmt.Errorf("failed to create request: %w", err)
		}

		// Set headers
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36")
		req.Header.Set("Accept", "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8")
		req.Header.Set("Accept-Language", config.AcceptLanguage)
		req.Header.Set("Cache-Control", "no-cache")
		req.Header.Set("Upgrade-Insecure-Requests", "1")

		resp, err := client.Do(req)
		if err != nil {
			lastErr = err
			if shouldRetry(err, 0) {
				continue
			}
			return nil, fmt.Errorf("failed to execute request: %w", err)
		}

		lastResp = resp

		if resp.StatusCode == http.StatusOK {
			return &FetchedDocument{
				URL:         rawURL,
				StatusCode:  resp.StatusCode,
				ContentType: resp.Header.Get("Content-Type"),
				Body:        resp.Body,
				FetchedAt:   time.Now(),
				Headers:     resp.Header,
			}, nil
		}

		// Check if we should retry this status code
		if shouldRetry(nil, resp.StatusCode) {
			resp.Body.Close()
			lastErr = fmt.Errorf("status code %d", resp.StatusCode)
			continue
		}

		// Non-retryable error
		resp.Body.Close()
		return nil, fmt.Errorf("unexpected status code: %d", resp.StatusCode)
	}

	// All retries exhausted
	if lastResp != nil {
		lastResp.Body.Close()
	}
	return nil, fmt.Errorf("max retries exceeded: %w", lastErr)
}
