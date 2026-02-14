-- Migration 013: Performance indexes for active listing, filters, FTS and vector similarity

-- Active status partial indexes
CREATE INDEX IF NOT EXISTS idx_opp_active_rolling
ON opportunities (updated_at DESC, created_at DESC)
WHERE COALESCE(opp_status, 'posted') NOT IN ('closed', 'archived', 'funded')
  AND is_rolling = true;

CREATE INDEX IF NOT EXISTS idx_opp_active_deadline
ON opportunities (deadline_at ASC, updated_at DESC)
WHERE COALESCE(opp_status, 'posted') NOT IN ('closed', 'archived', 'funded')
  AND deadline_at IS NOT NULL;

CREATE INDEX IF NOT EXISTS idx_opp_active_null_deadline_recent
ON opportunities (updated_at DESC, created_at DESC)
WHERE COALESCE(opp_status, 'posted') NOT IN ('closed', 'archived', 'funded')
  AND deadline_at IS NULL;

-- Array filters
CREATE INDEX IF NOT EXISTS idx_opp_categories_gin ON opportunities USING GIN (categories);
CREATE INDEX IF NOT EXISTS idx_opp_eligibility_gin ON opportunities USING GIN (eligibility);

-- Ensure FTS index exists
CREATE INDEX IF NOT EXISTS idx_opportunities_search_vector ON opportunities USING GIN (search_vector);

-- Vector index for non-null embeddings only (null embeddings stay queryable via COALESCE fallback ordering)
CREATE INDEX IF NOT EXISTS idx_opp_embedding_hnsw_nonnull
ON opportunities USING hnsw (embedding vector_cosine_ops)
WHERE embedding IS NOT NULL;
