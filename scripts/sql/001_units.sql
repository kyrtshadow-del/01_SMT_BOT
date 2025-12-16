-- Units and configs registry
CREATE TABLE IF NOT EXISTS units (
    id INTEGER PRIMARY KEY,
    name TEXT NOT NULL,
    uid TEXT UNIQUE,
    hw_type TEXT,
    node_id INTEGER,
    is_deleted BOOLEAN DEFAULT FALSE,
    created_at TIMESTAMPTZ DEFAULT NOW(),
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS ix_units_uid ON units(uid);
CREATE INDEX IF NOT EXISTS ix_units_node ON units(node_id);

CREATE TABLE IF NOT EXISTS unit_configs (
    unit_id INTEGER PRIMARY KEY REFERENCES units(id) ON DELETE CASCADE,
    config JSONB NOT NULL DEFAULT '{}'::jsonb,
    updated_at TIMESTAMPTZ DEFAULT NOW()
);

CREATE OR REPLACE FUNCTION update_timestamp()
RETURNS TRIGGER AS $$
BEGIN
    NEW.updated_at = NOW();
    RETURN NEW;
END;
$$ LANGUAGE plpgsql;

CREATE TRIGGER trg_units_updated
BEFORE UPDATE ON units
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();

CREATE TRIGGER trg_unit_configs_updated
BEFORE UPDATE ON unit_configs
FOR EACH ROW EXECUTE PROCEDURE update_timestamp();
