-- Setup recipe_embeddings table
-- This table stores vector embeddings computed from recipe text chunks
-- Uses pgvector for efficient similarity search
-- Enables semantic search over recipe instructions, ingredients, and descriptions

-- Enable pgvector extension (should already be enabled in Lakebase)
CREATE EXTENSION IF NOT EXISTS vector;

-- Create embeddings table with vector column
CREATE TABLE IF NOT EXISTS recipe_embeddings (
    id SERIAL PRIMARY KEY,
    external_id_meal INTEGER NOT NULL,  -- FK to recipe.external_id_meal
    guid_recipe UUID,  -- Optional FK to recipe.id_guid_sk for additional joins
    chunk_index INTEGER NOT NULL,
    chunk_text TEXT NOT NULL,
    embedding vector(384) NOT NULL,  -- 384-dim for all-MiniLM-L6-v2
    model_name TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    UNIQUE (external_id_meal, chunk_index)
);

-- Index on external_id_meal for fast joins with recipe table
CREATE INDEX IF NOT EXISTS idx_recipe_embeddings_external_id 
ON recipe_embeddings (external_id_meal);

-- Index on guid_recipe for alternative join path
CREATE INDEX IF NOT EXISTS idx_recipe_embeddings_guid_recipe 
ON recipe_embeddings (guid_recipe);

-- HNSW index for fast cosine similarity search
-- This enables efficient vector search using the <=> operator
CREATE INDEX IF NOT EXISTS idx_recipe_embeddings_embedding_cosine
ON recipe_embeddings
USING hnsw (embedding vector_cosine_ops);

-- Alternative: IVFFlat index (faster build, slightly slower search)
-- Uncomment if you prefer this index type:
-- CREATE INDEX IF NOT EXISTS idx_recipe_embeddings_embedding_ivfflat
-- ON recipe_embeddings
-- USING ivfflat (embedding vector_cosine_ops)
-- WITH (lists = 100);