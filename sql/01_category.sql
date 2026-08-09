-- Setup category table
-- This table stores meal categories from TheMealDB API
-- Categories represent different types of meals (e.g., Beef, Chicken, Dessert)

CREATE TABLE IF NOT EXISTS category (
    id_guid_sk UUID PRIMARY KEY,
    external_id INTEGER NOT NULL,
    name TEXT NOT NULL,
    description TEXT,
    imagem TEXT,
    __source TEXT NOT NULL DEFAULT 'themeal_db',
    __ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    __updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index on external_id for fast lookups from API
CREATE INDEX IF NOT EXISTS idx_category_external_id 
ON category (external_id);

-- Index on name for joins with recipe table
CREATE INDEX IF NOT EXISTS idx_category_name 
ON category (name);

-- Index on updated_at for incremental processing
CREATE INDEX IF NOT EXISTS idx_category_updated_at 
ON category (__updated_at);
