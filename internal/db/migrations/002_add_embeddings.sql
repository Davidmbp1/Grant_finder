-- Enable pgvector extension
CREATE EXTENSION IF NOT EXISTS vector;

-- Add embedding column to opportunities table
-- nomic-embed-text uses 768 dimensions
ALTER TABLE opportunities ADD COLUMN IF NOT EXISTS embedding vector(768);

-- Create HNSW index for fast semantic search
CREATE INDEX IF NOT EXISTS opportunities_embedding_idx ON opportunities USING hnsw (embedding vector_cosine_ops);
