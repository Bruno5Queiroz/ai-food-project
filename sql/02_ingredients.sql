-- Setup ingredients table
-- This table stores all available ingredients from TheMealDB API
-- Ingredients can be used to filter and search recipes

CREATE TABLE IF NOT EXISTS ingredients (
    id_guid_sk UUID PRIMARY KEY,
    external_id INTEGER NOT NULL,
    ingredient_name TEXT NOT NULL,
    description TEXT,
    type_ingredient TEXT,
    image TEXT,
    __source TEXT NOT NULL DEFAULT 'themeal_db',
    __ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    __updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index on external_id for fast lookups from API
CREATE INDEX IF NOT EXISTS idx_ingredients_external_id 
ON ingredients (external_id);

-- Index on ingredient_name for text search and filtering
CREATE INDEX IF NOT EXISTS idx_ingredients_name 
ON ingredients (ingredient_name);

-- Index on type for filtering by ingredient type
CREATE INDEX IF NOT EXISTS idx_ingredients_type 
ON ingredients (type_ingredient);

-- Index on updated_at for incremental processing
CREATE INDEX IF NOT EXISTS idx_ingredients_updated_at 
ON ingredients (__updated_at);