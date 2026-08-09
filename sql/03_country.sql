-- Setup country table
-- This table stores countries/cuisines from TheMealDB API
-- Maps meal origins to their respective countries and regions

CREATE TABLE IF NOT EXISTS country (
    id_guid_sk UUID PRIMARY KEY,
    country TEXT NOT NULL,
    cuisine TEXT,
    acronym TEXT,
    region TEXT,
    image TEXT,
    __source TEXT NOT NULL DEFAULT 'themeal_db',
    __ingested_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    __updated_at TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Index on country name for joins with recipe table
CREATE INDEX IF NOT EXISTS idx_country_name 
ON country (country);

-- Index on cuisine for filtering by cuisine type
CREATE INDEX IF NOT EXISTS idx_country_cuisine 
ON country (cuisine);

-- Index on region for filtering by geographical area
CREATE INDEX IF NOT EXISTS idx_country_region 
ON country (region);

-- Index on acronym for ISO country code lookups
CREATE INDEX IF NOT EXISTS idx_country_acronym 
ON country (acronym);

-- Index on updated_at for incremental processing
CREATE INDEX IF NOT EXISTS idx_country_updated_at 
ON country (__updated_at);