-- Setup recipe table
-- This table stores complete recipe information from TheMealDB API
-- Joins data from recipe, category, country, and fully_recipe sources

CREATE TABLE IF NOT EXISTS recipe (
    id_guid_sk UUID PRIMARY KEY,
    external_id_meal INTEGER,  -- Allow NULL for user-added recipes
    name_meal TEXT NOT NULL,
    guid_category UUID NOT NULL,  -- FK to category.id_guid_sk
    country TEXT NOT NULL,
    region TEXT,
    instructions TEXT,
    ingredient_measure TEXT,  -- JSON/TEXT field with ingredient quantities
    image_recipe TEXT,
    video_recipe TEXT,
    source_url TEXT,
    date_modified TEXT,
    __source TEXT NOT NULL DEFAULT 'themeal_db',
    __ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    __updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index on external_id for fast lookups from API
CREATE INDEX IF NOT EXISTS idx_recipe_external_id 
ON recipe (external_id_meal);

-- Index on category for filtering by category
CREATE INDEX IF NOT EXISTS idx_recipe_category 
ON recipe (guid_category);

-- Index on country for filtering by origin
CREATE INDEX IF NOT EXISTS idx_recipe_country 
ON recipe (country);

-- Index on region for geographical filtering
CREATE INDEX IF NOT EXISTS idx_recipe_region 
ON recipe (region);

-- Index on name for text search
CREATE INDEX IF NOT EXISTS idx_recipe_name 
ON recipe (name_meal);

-- Index on updated_at for incremental processing
CREATE INDEX IF NOT EXISTS idx_recipe_updated_at 
ON recipe (__updated_at);

-- Index on date_modified for tracking recipe changes
CREATE INDEX IF NOT EXISTS idx_recipe_date_modified 
ON recipe (date_modified);