package api

import (
	"context"
	"crypto/rand"
	"encoding/base64"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/david/grant-finder/internal/ai"
	"github.com/david/grant-finder/internal/auth"
	"github.com/david/grant-finder/internal/db"
	"github.com/david/grant-finder/internal/ingest"
	"github.com/david/grant-finder/internal/models"
	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/labstack/echo/v4"
	"github.com/labstack/echo/v4/middleware"
)

type Server struct {
	Store       *db.Store
	AuthService *auth.Service
	Echo        *echo.Echo
	DB          *pgxpool.Pool
	AI          *ai.OllamaClient

	// Background job tracking
	jobMu      sync.Mutex
	runningJob *backgroundJob
}

type backgroundJob struct {
	ID        string    `json:"id"`
	Status    string    `json:"status"` // running, completed, failed
	StartedAt time.Time `json:"started_at"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	Result    any       `json:"result,omitempty"`
	Error     string    `json:"error,omitempty"`
	Cancel    context.CancelFunc `json:"-"`
}

var (
	adminSecretOnce    sync.Once
	adminSecretRuntime string
	adminSecretErr     error
)

func NewServer(pool *pgxpool.Pool) *Server {
	e := echo.New()
	e.Use(middleware.Logger())
	e.Use(middleware.Recover())

	// CORS: allow frontend origins from env or default to localhost
	allowedOrigins := []string{"http://localhost:4200"}
	if extra := os.Getenv("CORS_ORIGINS"); extra != "" {
		for _, o := range strings.Split(extra, ",") {
			o = strings.TrimSpace(o)
			if o != "" {
				allowedOrigins = append(allowedOrigins, o)
			}
		}
	}
	e.Use(middleware.CORSWithConfig(middleware.CORSConfig{
		AllowOrigins: allowedOrigins,
		AllowMethods: []string{http.MethodGet, http.MethodPost, http.MethodPatch, http.MethodDelete},
		AllowHeaders: []string{echo.HeaderOrigin, echo.HeaderContentType, echo.HeaderAccept, echo.HeaderAuthorization, "X-Admin-Secret"},
	}))

	store := db.NewStore(pool)
	authService := auth.NewService(pool)

	// Initialize AI client once
	ollamaHost := os.Getenv("OLLAMA_HOST")
	if ollamaHost == "" {
		ollamaHost = "http://localhost:11434"
	}
	aiClient := ai.NewOllamaClient(ollamaHost, "", "qwen2.5:14b")

	s := &Server{
		DB:          pool,
		Store:       store,
		AuthService: authService,
		Echo:        e,
		AI:          aiClient,
	}

	s.routes()
	return s
}

func (s *Server) routes() {
	s.Echo.GET("/health", s.handleHealth)
	api := s.Echo.Group("/api/v1")
	api.GET("/opportunities", s.handleListOpportunities)
	api.GET("/opportunities/:id", s.handleGetOpportunity)
	api.GET("/sources", s.handleGetSources)
	// Public Stats
	api.GET("/stats", s.handleGetStats)
	api.GET("/aggregations", s.handleGetAggregations)

	// Admin Routes (Ingest & Seed)
	admin := api.Group("")
	admin.Use(s.adminMiddleware)
	admin.POST("/ingest", s.handleTriggerIngest)
	admin.POST("/ingest/grantsgov", s.handleIngestGrantsGov)
	admin.POST("/ingest/nih", s.handleIngestNIH)
	admin.POST("/ingest/nsf", s.handleIngestNSF)
	admin.POST("/ingest/openalex", s.handleIngestOpenAlex)
	admin.POST("/ingest/ukri", s.handleIngestUKRI)
	admin.POST("/ingest/source/:id", s.handleIngestSourceByID)
	admin.POST("/ingest/all", s.handleIngestAll)
	admin.POST("/seed", s.handleSeed)
	admin.POST("/admin/refine-data", s.handleRefineData)
	admin.POST("/admin/recompute-status", s.handleRecomputeStatus)
	admin.GET("/admin/job/:id", s.handleJobStatus)
	admin.POST("/admin/enrich-opportunities", s.handleEnrichOpportunities)

	// Auth Routes
	api.POST("/auth/signup", s.handleSignup)
	api.POST("/auth/login", s.handleLogin)

	// Protected Routes (Saved Opportunities)
	saved := api.Group("/saved")
	saved.Use(auth.Middleware)
	saved.POST("/:id", s.handleSaveOpportunity)
	saved.DELETE("/:id", s.handleUnsaveOpportunity)
	saved.GET("", s.handleGetSavedOpportunities)
}

func (s *Server) handleSignup(c echo.Context) error {
	var req auth.SignupRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}
	// TODO: Add strict validation here

	resp, err := s.AuthService.Signup(c.Request().Context(), req)
	if err != nil {
		if err == auth.ErrUserExists {
			return c.JSON(http.StatusConflict, map[string]string{"error": err.Error()})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusCreated, resp)
}

func (s *Server) handleLogin(c echo.Context) error {
	var req auth.LoginRequest
	if err := c.Bind(&req); err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid request"})
	}

	resp, err := s.AuthService.Login(c.Request().Context(), req)
	if err != nil {
		if err == auth.ErrInvalidCreds {
			return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Invalid credentials"})
		}
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleGetAggregations(c echo.Context) error {
	params := db.AggregationParams{
		Status: c.QueryParam("status"),
	}
	if v := c.QueryParam("region"); v != "" {
		params.Region = splitCSV(v)
	}
	if v := c.QueryParam("funder_type"); v != "" {
		params.FunderType = splitCSV(v)
	}
	if v := c.QueryParam("country"); v != "" {
		params.Country = splitCSV(v)
	}
	if v := c.QueryParam("agency_name"); v != "" {
		params.AgencyName = splitCSV(v)
	}
	aggs, err := s.Store.GetAggregations(c.Request().Context(), params)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, aggs)
}

// splitCSV splits a comma-separated query parameter into trimmed non-empty strings.
func splitCSV(s string) []string {
	var result []string
	for _, part := range strings.Split(s, ",") {
		part = strings.TrimSpace(part)
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func (s *Server) handleHealth(c echo.Context) error {
	return c.String(http.StatusOK, "OK")
}

func (s *Server) handleListOpportunities(c echo.Context) error {
	q := c.QueryParam("q")
	source := c.QueryParam("source")
	region := c.QueryParam("region")
	funderType := c.QueryParam("funder_type")
	// These are now multi-value CSV
	country := c.QueryParam("country")
	agencyCode := c.QueryParam("agency_code")
	agencyName := c.QueryParam("agency_name")
	limitStr := c.QueryParam("limit")
	offsetStr := c.QueryParam("offset")
	minAmountStr := c.QueryParam("min_amount")
	maxAmountStr := c.QueryParam("max_amount")
	deadlineDaysStr := c.QueryParam("deadline_days")
	isRollingStr := c.QueryParam("is_rolling")
	categories := c.QueryParams()["categories"]
	eligibility := c.QueryParams()["eligibility"]
	sortBy := c.QueryParam("sort")
	status := c.QueryParam("status")

	limit := 20
	offset := 0
	var minAmount, maxAmount float64
	var deadlineDays int
	var isRolling *bool

	if l, err := strconv.Atoi(limitStr); err == nil && l > 0 && l <= 100 {
		limit = l
	}
	if o, err := strconv.Atoi(offsetStr); err == nil && o >= 0 {
		offset = o
	}
	if v, err := strconv.ParseFloat(minAmountStr, 64); err == nil && v > 0 {
		minAmount = v
	}
	if v, err := strconv.ParseFloat(maxAmountStr, 64); err == nil && v > 0 {
		maxAmount = v
	}
	if v, err := strconv.Atoi(deadlineDaysStr); err == nil && v > 0 {
		deadlineDays = v
	}
	if isRollingStr != "" {
		val := isRollingStr == "true"
		isRolling = &val
	}

	// Generate embedding for semantic search
	var queryEmbedding []float32
	if q != "" {
		// Create a context with timeout for AI operation
		aiCtx, cancel := context.WithTimeout(c.Request().Context(), 5*time.Second)
		defer cancel()

		vec, err := s.AI.GenerateEmbedding(aiCtx, q)
		if err != nil {
			c.Logger().Errorf("Failed to generate query embedding: %v", err)
			// Apply fallback: proceed with keyword search (queryEmbedding remains nil)
		} else {
			queryEmbedding = vec
		}
	}

	result, err := s.Store.ListOpportunities(c.Request().Context(), db.ListParams{
		Query:          q,
		QueryEmbedding: queryEmbedding,
		Source:         source,
		Region:         splitCSV(region),
		FunderType:     splitCSV(funderType),
		Country:        splitCSV(country),
		AgencyCode:     agencyCode,
		AgencyName:     splitCSV(agencyName),
		MinAmount:      minAmount,
		MaxAmount:      maxAmount,
		DeadlineDays:   deadlineDays,
		IsRolling:      isRolling,
		Limit:          limit,
		Offset:         offset,
		Categories:     categories,
		Eligibility:    eligibility,
		SortBy:         sortBy,
		Status:         status,
	})
	if err != nil {
		c.Logger().Errorf("Failed to list opportunities: %v", err)
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Internal Server Error"})
	}

	return c.JSON(http.StatusOK, result)
}

func (s *Server) handleGetSources(c echo.Context) error {
	sources, err := s.Store.GetSources(c.Request().Context())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, sources)
}

func (s *Server) handleGetStats(c echo.Context) error {
	stats, err := s.Store.GetStats(c.Request().Context())
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}
	return c.JSON(http.StatusOK, stats)
}

func (srv *Server) handleGetOpportunity(c echo.Context) error {
	id := c.Param("id")
	opp, err := srv.Store.GetOpportunity(c.Request().Context(), id)
	if err != nil {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "Not found"})
	}
	return c.JSON(http.StatusOK, opp)
}

func (s *Server) handleTriggerIngest(c echo.Context) error {
	urlStr := c.QueryParam("url")
	if urlStr == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "url param required"})
	}

	u, err := url.Parse(urlStr)
	if err != nil || (u.Scheme != "http" && u.Scheme != "https") {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid URL scheme"})
	}
	host := strings.ToLower(u.Hostname())
	if host == "" {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "URL host is required"})
	}
	if host == "localhost" || host == "127.0.0.1" || host == "::1" || strings.HasSuffix(host, ".local") {
		return c.JSON(http.StatusForbidden, map[string]string{"error": "Internal network access forbidden"})
	}

	ips, err := net.LookupIP(host)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Unable to resolve URL host"})
	}
	if len(ips) == 0 {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "URL host resolved to no addresses"})
	}
	for _, ip := range ips {
		if isPrivateOrSpecialIP(ip) {
			return c.JSON(http.StatusForbidden, map[string]string{"error": "Internal network access forbidden"})
		}
	}

	fetcher := ingest.NewHTTPFetcher()
	parser := ingest.NewOllamaParser("qwen2.5:14b")
	pipeline := ingest.NewPipeline(s.DB, fetcher, parser, s.AI)

	// Run synchronously for MVP debugging
	if err := pipeline.Run(c.Request().Context(), urlStr); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]string{"message": "Ingestion complete", "url": urlStr})
}

func (s *Server) handleIngestGrantsGov(c echo.Context) error {
	// Map legacy endpoint to new registry ID
	return s.runIngestionForSource(c, "grants_gov")
}

func (s *Server) handleIngestNIH(c echo.Context) error {
	// NIH is currently disabled/not in registry active list
	return c.JSON(http.StatusBadRequest, map[string]string{"error": "NIH ingestion is disabled in registry"})
}

func (s *Server) handleIngestNSF(c echo.Context) error {
	return c.JSON(http.StatusBadRequest, map[string]string{"error": "NSF ingestion is disabled in registry"})
}

func (s *Server) handleIngestOpenAlex(c echo.Context) error {
	return c.JSON(http.StatusBadRequest, map[string]string{"error": "OpenAlex ingestion is disabled in registry"})
}

func (s *Server) handleIngestUKRI(c echo.Context) error {
	// If UKRI is added to registry later, we use that.
	return c.JSON(http.StatusBadRequest, map[string]string{"error": "UKRI ingestion pending registry migration"})
}

func (s *Server) handleIngestSourceByID(c echo.Context) error {
	sourceID := c.Param("id")
	return s.runIngestionForSource(c, sourceID)
}

func (s *Server) handleIngestAll(c echo.Context) error {
	pipeline := ingest.NewPipeline(s.DB, nil, nil, s.AI)
	ctx := c.Request().Context()

	results, err := pipeline.IngestAll(ctx)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": "All registry sources ingestion complete",
		"results": results,
	})
}

// Helper to run a specific source from registry
func (s *Server) runIngestionForSource(c echo.Context, sourceID string) error {
	pipeline := ingest.NewPipeline(s.DB, nil, nil, s.AI)

	stats, err := pipeline.IngestSource(c.Request().Context(), sourceID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": fmt.Sprintf("%s ingestion complete", sourceID),
		"stats":   stats,
	})
}

func (s *Server) handleRefineData(c echo.Context) error {
	pipeline := ingest.NewPipeline(s.DB, nil, nil, s.AI)
	ctx := c.Request().Context()

	updated, err := pipeline.RefineAllData(ctx)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": "Data refinement complete",
		"updated": updated,
	})
}

func (s *Server) handleRecomputeStatus(c echo.Context) error {
	s.jobMu.Lock()
	if s.runningJob != nil && s.runningJob.Status == "running" {
		job := s.runningJob
		s.jobMu.Unlock()
		return c.JSON(http.StatusConflict, map[string]interface{}{
			"error":  "A recompute job is already running",
			"job_id": job.ID,
		})
	}

	batchSize := 500
	if raw := strings.TrimSpace(c.QueryParam("batch_size")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 5000 {
			batchSize = parsed
		}
	}

	// context.WithoutCancel detaches from HTTP lifecycle but preserves
	// trace values. We add our own timeout for safety.
	jobCtx, jobCancel := context.WithTimeout(
		context.WithoutCancel(c.Request().Context()), 30*time.Minute,
	)

	jobID := uuid.New().String()[:8]
	job := &backgroundJob{
		ID:        jobID,
		Status:    "running",
		StartedAt: time.Now(),
		Cancel:    jobCancel,
	}
	s.runningJob = job
	s.jobMu.Unlock()

	// Run in background goroutine — returns 202 immediately.
	go func() {
		defer jobCancel()
		pipeline := ingest.NewPipeline(s.DB, nil, nil, s.AI)

		statusCounts, statusUpdated, err := pipeline.RecomputeStatuses(jobCtx, batchSize)
		if err != nil {
			s.jobMu.Lock()
			job.Status = "failed"
			job.Error = err.Error()
			job.EndedAt = time.Now()
			s.jobMu.Unlock()
			log.Printf("[recompute-job %s] failed: %v", jobID, err)
			return
		}

		arraysUpdated, _ := pipeline.BackfillCleanArrays(jobCtx)

		s.jobMu.Lock()
		job.Status = "completed"
		job.EndedAt = time.Now()
		job.Result = map[string]interface{}{
			"status_updated":  statusUpdated,
			"status_counts":   statusCounts,
			"arrays_updated":  arraysUpdated,
			"batch_size_used": batchSize,
		}
		s.jobMu.Unlock()
		log.Printf("[recompute-job %s] completed: updated=%d", jobID, statusUpdated)
	}()

	return c.JSON(http.StatusAccepted, map[string]interface{}{
		"message": "Recompute job started",
		"job_id":  jobID,
		"poll":    fmt.Sprintf("/api/v1/admin/job/%s", jobID),
	})
}

func (s *Server) handleJobStatus(c echo.Context) error {
	queried := c.Param("id")
	s.jobMu.Lock()
	job := s.runningJob
	s.jobMu.Unlock()

	if job == nil || job.ID != queried {
		return c.JSON(http.StatusNotFound, map[string]string{"error": "job not found"})
	}

	s.jobMu.Lock()
	resp := map[string]interface{}{
		"id":         job.ID,
		"status":     job.Status,
		"started_at": job.StartedAt,
	}
	if !job.EndedAt.IsZero() {
		resp["ended_at"] = job.EndedAt
		resp["duration"] = job.EndedAt.Sub(job.StartedAt).String()
	}
	if job.Result != nil {
		resp["result"] = job.Result
	}
	if job.Error != "" {
		resp["error"] = job.Error
	}
	s.jobMu.Unlock()

	return c.JSON(http.StatusOK, resp)
}

func (s *Server) handleEnrichOpportunities(c echo.Context) error {
	pipeline := ingest.NewPipeline(s.DB, nil, nil, s.AI)
	ctx := c.Request().Context()

	domain := strings.TrimSpace(c.QueryParam("domain"))
	onlyMissingDeadlines := true
	if raw := strings.TrimSpace(c.QueryParam("only_missing_deadlines")); raw != "" {
		onlyMissingDeadlines = strings.EqualFold(raw, "true")
	}

	batchSize := 200
	if raw := strings.TrimSpace(c.QueryParam("batch_size")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 2000 {
			batchSize = parsed
		}
	}

	maxItems := batchSize
	if raw := strings.TrimSpace(c.QueryParam("max_items")); raw != "" {
		if parsed, err := strconv.Atoi(raw); err == nil && parsed > 0 && parsed <= 10000 {
			maxItems = parsed
		}
	}

	confidenceThreshold := 0.6
	if raw := strings.TrimSpace(c.QueryParam("confidence_threshold")); raw != "" {
		if parsed, err := strconv.ParseFloat(raw, 64); err == nil && parsed >= 0 && parsed <= 1 {
			confidenceThreshold = parsed
		}
	}

	enrichStats, err := pipeline.EnrichOpportunities(ctx, domain, onlyMissingDeadlines, batchSize, maxItems, confidenceThreshold)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	statusCounts, statusUpdated, err := pipeline.RecomputeStatuses(ctx, batchSize)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": err.Error()})
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message":                "Selective enrichment complete",
		"domain":                 domain,
		"only_missing_deadlines": onlyMissingDeadlines,
		"batch_size_used":        batchSize,
		"max_items":              maxItems,
		"confidence_threshold":   confidenceThreshold,
		"items_scanned":          enrichStats.ItemsScanned,
		"items_updated":          enrichStats.ItemsUpdated,
		"pdfs_parsed":            enrichStats.PDFsParsed,
		"deadlines_added":        enrichStats.DeadlinesAdded,
		"status_changes":         enrichStats.StatusChanges,
		"status_updated":         statusUpdated,
		"status_counts":          statusCounts,
	})
}

func (s *Server) handleSeed(c echo.Context) error {
	ctx := c.Request().Context()

	seeds := []struct {
		Title       string
		Summary     string
		Description string
		URL         string
		Domain      string
		AmountMin   float64
		AmountMax   float64
		Currency    string
		Deadline    *time.Time
		IsRolling   bool
	}{
		{
			Title:       "Gates Foundation Grand Challenges - Global Health Innovation",
			Summary:     "Grants for innovative solutions addressing global health challenges in low-income countries.",
			Description: "The Bill & Melinda Gates Foundation seeks bold ideas that explore innovative approaches to global health. Awards support early-stage research and proof-of-concept projects.",
			URL:         "https://gcgh.grandchallenges.org/grants",
			Domain:      "grandchallenges.org",
			AmountMin:   50000,
			AmountMax:   100000,
			Currency:    "USD",
			IsRolling:   true,
		},
		{
			Title:       "EU Horizon Europe - Climate Neutral Cities 2030",
			Summary:     "Funding for cities developing pathways to climate neutrality by 2030.",
			Description: "Part of the European Commission's Horizon Europe programme. Supports urban transformation projects including clean energy, sustainable mobility, and circular economy initiatives across EU member states.",
			URL:         "https://ec.europa.eu/info/funding-tenders/opportunities/portal/screen/opportunities/climate-neutral-cities",
			Domain:      "ec.europa.eu",
			AmountMin:   500000,
			AmountMax:   2000000,
			Currency:    "EUR",
			Deadline:    timePtr(time.Date(2026, 6, 15, 17, 0, 0, 0, time.UTC)),
		},
		{
			Title:       "USAID Development Innovation Ventures (DIV)",
			Summary:     "Tiered funding model for cost-effective, evidence-based solutions to development challenges.",
			Description: "DIV invests in breakthrough solutions to the world's most intractable development challenges. Funding ranges from pilot to scale across sectors including agriculture, education, health, and economic growth.",
			URL:         "https://www.usaid.gov/div",
			Domain:      "usaid.gov",
			AmountMin:   25000,
			AmountMax:   15000000,
			Currency:    "USD",
			Deadline:    timePtr(time.Date(2026, 9, 30, 23, 59, 0, 0, time.UTC)),
		},
		{
			Title:       "Google.org Impact Challenge: AI for Social Good",
			Summary:     "Funding and mentorship for organizations using AI to address societal challenges.",
			Description: "Google.org invites nonprofits, social enterprises, and research institutions to propose how they would use AI to create positive social impact. Selected projects receive funding, Google Cloud credits, and mentorship from Google AI experts.",
			URL:         "https://impactchallenge.withgoogle.com/ai-for-social-good",
			Domain:      "withgoogle.com",
			AmountMin:   100000,
			AmountMax:   2000000,
			Currency:    "USD",
			Deadline:    timePtr(time.Date(2026, 4, 1, 23, 59, 0, 0, time.UTC)),
		},
		{
			Title:       "Wellcome Trust - Discovery Research Grant",
			Summary:     "Supports established researchers pursuing novel questions in biomedical science.",
			Description: "Wellcome's Discovery Research scheme provides funding for experienced researchers to pursue important questions in science, spanning basic biology to population health.",
			URL:         "https://wellcome.org/grant-funding/schemes/discovery-research",
			Domain:      "wellcome.org",
			AmountMin:   300000,
			AmountMax:   3500000,
			Currency:    "GBP",
			IsRolling:   true,
		},
		{
			Title:       "Inter-American Development Bank (IDB) - Social Innovation Fund",
			Summary:     "Grants for innovative social projects in Latin America and the Caribbean.",
			Description: "The IDB's Social Innovation Fund supports the design, implementation, and scaling of innovative solutions to persistent social challenges in the LAC region, including poverty, inequality, and exclusion.",
			URL:         "https://www.iadb.org/en/sector/social-investment/social-innovation",
			Domain:      "iadb.org",
			AmountMin:   10000,
			AmountMax:   150000,
			Currency:    "USD",
			Deadline:    timePtr(time.Date(2026, 7, 31, 23, 59, 0, 0, time.UTC)),
		},
		{
			Title:       "Ford Foundation - Creativity and Free Expression",
			Summary:     "Support for artists, cultural organizations, and media advancing social justice narratives.",
			Description: "The Ford Foundation supports creative work that challenges inequality and advances understanding across cultures. Grants are available for film, visual arts, literature, journalism, and digital media.",
			URL:         "https://www.fordfoundation.org/work/challenging-inequality/creativity-and-free-expression/",
			Domain:      "fordfoundation.org",
			AmountMin:   50000,
			AmountMax:   500000,
			Currency:    "USD",
			IsRolling:   true,
		},
		{
			Title:       "UK Research and Innovation (UKRI) - Future Leaders Fellowships",
			Summary:     "Fellowships for early-career researchers and innovators with potential to be future leaders.",
			Description: "UKRI Future Leaders Fellowships are designed to develop the careers of world-class researchers and innovators across business and academia. Awards of up to £1.5m over 4 years for ambitious research and innovation.",
			URL:         "https://www.ukri.org/opportunity/future-leaders-fellowships-round-9/",
			Domain:      "ukri.org",
			AmountMin:   400000,
			AmountMax:   1500000,
			Currency:    "GBP",
			Deadline:    timePtr(time.Date(2026, 5, 20, 16, 0, 0, 0, time.UTC)),
		},
		{
			Title:       "MIT Solve - Global Challenges 2026",
			Summary:     "Prize-based challenges for tech-driven solutions to global issues including health, climate, and equity.",
			Description: "MIT Solve connects social entrepreneurs with funding, mentorship, and resources to scale their impact. Open to any organization or individual worldwide with a technology-based solution.",
			URL:         "https://solve.mit.edu/challenges",
			Domain:      "solve.mit.edu",
			AmountMin:   10000,
			AmountMax:   50000,
			Currency:    "USD",
			Deadline:    timePtr(time.Date(2026, 3, 15, 23, 59, 0, 0, time.UTC)),
		},
		{
			Title:       "Skoll Foundation Award for Social Entrepreneurship",
			Summary:     "Recognizes and invests in social entrepreneurs driving large-scale, systemic change.",
			Description: "The Skoll Award supports proven social entrepreneurs whose organizations are achieving transformational impact on critical social issues. Recipients receive multi-year core funding and access to the Skoll community.",
			URL:         "https://skoll.org/about/skoll-awards/",
			Domain:      "skoll.org",
			AmountMin:   500000,
			AmountMax:   1500000,
			Currency:    "USD",
			Deadline:    timePtr(time.Date(2026, 8, 1, 23, 59, 0, 0, time.UTC)),
		},
	}

	count := 0
	for _, seed := range seeds {
		query := `
			INSERT INTO opportunities (
				title, summary, description_html, external_url, source_domain,
				amount_min, amount_max, currency, deadline_at, is_rolling
			) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10)
			ON CONFLICT (external_url) DO UPDATE SET
				updated_at = NOW(),
				title = EXCLUDED.title,
				summary = EXCLUDED.summary,
				amount_min = EXCLUDED.amount_min,
				amount_max = EXCLUDED.amount_max
		`
		_, err := s.DB.Exec(ctx, query,
			seed.Title, seed.Summary, seed.Description, seed.URL, seed.Domain,
			seed.AmountMin, seed.AmountMax, seed.Currency, seed.Deadline, seed.IsRolling,
		)
		if err != nil {
			c.Logger().Errorf("Failed to seed: %v", err)
		}
		count++
	}

	return c.JSON(http.StatusOK, map[string]interface{}{
		"message": "Seed complete",
		"count":   count,
	})
}

func timePtr(t time.Time) *time.Time {
	return &t
}

// Protected Handlers

func (s *Server) handleSaveOpportunity(c echo.Context) error {
	ctx := c.Request().Context()
	userID, err := auth.GetUserIDFromContext(c)
	if err != nil {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
	}

	idStr := c.Param("id")
	oppID, err := uuid.Parse(idStr)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid opportunity ID"})
	}

	if err := s.AuthService.SaveOpportunity(ctx, userID, oppID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to save opportunity"})
	}

	return c.NoContent(http.StatusOK)
}

func (s *Server) handleUnsaveOpportunity(c echo.Context) error {
	ctx := c.Request().Context()
	userID, err := auth.GetUserIDFromContext(c)
	if err != nil {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
	}

	idStr := c.Param("id")
	oppID, err := uuid.Parse(idStr)
	if err != nil {
		return c.JSON(http.StatusBadRequest, map[string]string{"error": "Invalid opportunity ID"})
	}

	if err := s.AuthService.UnsaveOpportunity(ctx, userID, oppID); err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to unsave opportunity"})
	}

	return c.JSON(http.StatusOK, map[string]string{"status": "unsaved"})
}

func (s *Server) handleGetSavedOpportunities(c echo.Context) error {
	ctx := c.Request().Context()
	userID, err := auth.GetUserIDFromContext(c)
	if err != nil {
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Unauthorized"})
	}

	opps, err := s.AuthService.GetSavedOpportunities(ctx, userID)
	if err != nil {
		return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Failed to fetch saved opportunities"})
	}

	if opps == nil {
		opps = []models.Opportunity{}
	}

	return c.JSON(http.StatusOK, opps)
}

func (s *Server) Start(port string) error {
	return s.Echo.Start(":" + port)
}

func isPrivateOrSpecialIP(ip net.IP) bool {
	if ip == nil {
		return true
	}
	if ip.IsLoopback() || ip.IsPrivate() || ip.IsUnspecified() || ip.IsMulticast() || ip.IsLinkLocalUnicast() || ip.IsLinkLocalMulticast() {
		return true
	}

	if ip4 := ip.To4(); ip4 != nil {
		if ip4[0] == 100 && ip4[1]&0xC0 == 64 {
			return true
		}
		if ip4[0] == 169 && ip4[1] == 254 {
			return true
		}
	}

	return false
}

func (s *Server) adminMiddleware(next echo.HandlerFunc) echo.HandlerFunc {
	return func(c echo.Context) error {
		secret, err := adminSecret()
		if err != nil {
			return c.JSON(http.StatusInternalServerError, map[string]string{"error": "Server admin configuration error"})
		}

		// Check X-Admin-Secret header or Bearer token
		authHeader := c.Request().Header.Get("Authorization")
		adminHeader := c.Request().Header.Get("X-Admin-Secret")

		if adminHeader == secret {
			return next(c)
		}
		if len(authHeader) > 7 && strings.EqualFold(authHeader[:7], "Bearer ") {
			if authHeader[7:] == secret {
				return next(c)
			}
		}

		// Also allow localhost for development convenience if no env var set (optional, but safer to stick to secret)
		// adhering to "secure by default": reject if no match
		return c.JSON(http.StatusUnauthorized, map[string]string{"error": "Unauthorized admin access"})
	}
}

func adminSecret() (string, error) {
	adminSecretOnce.Do(func() {
		secret := strings.TrimSpace(os.Getenv("ADMIN_SECRET"))
		if secret != "" {
			adminSecretRuntime = secret
			return
		}

		buf := make([]byte, 48)
		if _, err := rand.Read(buf); err != nil {
			adminSecretErr = fmt.Errorf("failed to generate ADMIN_SECRET fallback: %w", err)
			return
		}

		adminSecretRuntime = base64.RawURLEncoding.EncodeToString(buf)
		log.Print("ADMIN_SECRET is not set; using ephemeral in-memory fallback secret")
	})

	if adminSecretErr != nil {
		return "", adminSecretErr
	}
	if adminSecretRuntime == "" {
		return "", fmt.Errorf("admin secret unavailable")
	}

	return adminSecretRuntime, nil
}
