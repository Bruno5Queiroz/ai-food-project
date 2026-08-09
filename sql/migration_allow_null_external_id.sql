-- Migration: Allow NULL for external_id_meal to support user-added recipes
-- This allows recipes added through the UI to not require an external API ID

ALTER TABLE recipe 
ALTER COLUMN external_id_meal DROP NOT NULL;