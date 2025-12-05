-- SMT Telematics Core 2.0 — Phase 0 schema (PostgreSQL)
-- Source of truth for devices, units and active bindings.
-- Run once via psql or managed by a migration tool (Alembic later).

CREATE TABLE IF NOT EXISTS nodes (
    id            SERIAL PRIMARY KEY,
    parent_id     INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
    name          VARCHAR(255) NOT NULL,
    ord           INTEGER NOT NULL DEFAULT 0,
    created_at    TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at    TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_nodes_parent ON nodes(parent_id);

-- Logical objects (ТС). "sensors_config" keeps hybrid strategy config (versioned JSONB).
CREATE TABLE IF NOT EXISTS units (
    id              INTEGER PRIMARY KEY,
    name            VARCHAR(255) NOT NULL,
    reg_number      VARCHAR(64),
    owner_node_id   INTEGER REFERENCES nodes(id) ON DELETE SET NULL,
    sensors_config  JSONB NOT NULL DEFAULT '{"version":2,"sensors":[]}',
    created_at      TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at      TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_units_owner ON units(owner_node_id);

-- Physical devices (IMEI / hardware ID).
CREATE TABLE IF NOT EXISTS devices (
    id          SERIAL PRIMARY KEY,
    protocol    VARCHAR(32) NOT NULL,
    uid         VARCHAR(128) NOT NULL,
    hardware    VARCHAR(128),
    firmware    VARCHAR(128),
    is_active   BOOLEAN NOT NULL DEFAULT TRUE,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    UNIQUE(protocol, uid)
);
CREATE INDEX IF NOT EXISTS ix_devices_protocol_uid ON devices(protocol, uid);

-- Binding device -> unit with priority and validity window.
CREATE TABLE IF NOT EXISTS unit_device_links (
    id          SERIAL PRIMARY KEY,
    unit_id     INTEGER NOT NULL REFERENCES units(id) ON DELETE CASCADE,
    device_id   INTEGER NOT NULL REFERENCES devices(id) ON DELETE CASCADE,
    priority    INTEGER NOT NULL DEFAULT 50,
    valid_from  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    valid_to    TIMESTAMPTZ,
    created_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW()
);
CREATE INDEX IF NOT EXISTS ix_links_unit ON unit_device_links(unit_id);
CREATE INDEX IF NOT EXISTS ix_links_device ON unit_device_links(device_id);
-- Only one active binding per device (valid_to IS NULL).
CREATE UNIQUE INDEX IF NOT EXISTS ux_links_device_active ON unit_device_links(device_id) WHERE valid_to IS NULL;

-- Optional: history per unit (only one primary binding at a time).
CREATE UNIQUE INDEX IF NOT EXISTS ux_links_primary_active ON unit_device_links(unit_id, device_id) WHERE valid_to IS NULL;

-- Simple trigger placeholders for updated_at (can be replaced with Alembic migration)
-- Using psql: CREATE EXTENSION IF NOT EXISTS "uuid-ossp"; -- not required here.

