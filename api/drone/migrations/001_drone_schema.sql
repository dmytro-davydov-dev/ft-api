-- Phase 5: Drone 3D mapping schema
-- Apply against Supabase dev project: supabase db push

-- sites
CREATE TABLE IF NOT EXISTS sites (
  id            UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  customer_id   TEXT NOT NULL,
  name          TEXT NOT NULL,
  created_at    TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- captures
CREATE TABLE IF NOT EXISTS captures (
  id               UUID PRIMARY KEY DEFAULT gen_random_uuid(),
  site_id          UUID NOT NULL REFERENCES sites(id) ON DELETE CASCADE,
  customer_id      TEXT NOT NULL,
  captured_at      TIMESTAMPTZ NOT NULL,
  photo_count      INTEGER NOT NULL,
  status           TEXT NOT NULL DEFAULT 'pending',
    -- pending | uploading | processing | tiling | ready | error
  odm_task_id      TEXT,
  tiles_gcs_prefix TEXT,
  metadata         JSONB DEFAULT '{}',
  created_at       TIMESTAMPTZ NOT NULL DEFAULT now(),
  updated_at       TIMESTAMPTZ NOT NULL DEFAULT now()
);

-- Auto-update updated_at on captures
CREATE OR REPLACE FUNCTION set_updated_at()
RETURNS TRIGGER AS $$
BEGIN
  NEW.updated_at = now();
  RETURN NEW;
END;
$$ LANGUAGE plpgsql;

DROP TRIGGER IF EXISTS captures_set_updated_at ON captures;
CREATE TRIGGER captures_set_updated_at
  BEFORE UPDATE ON captures
  FOR EACH ROW EXECUTE FUNCTION set_updated_at();

-- Indexes
CREATE INDEX IF NOT EXISTS idx_captures_site_id     ON captures(site_id);
CREATE INDEX IF NOT EXISTS idx_captures_status      ON captures(status);
CREATE INDEX IF NOT EXISTS idx_captures_customer_id ON captures(customer_id);

-- RLS
ALTER TABLE sites    ENABLE ROW LEVEL SECURITY;
ALTER TABLE captures ENABLE ROW LEVEL SECURITY;

DROP POLICY IF EXISTS "tenant_isolation" ON sites;
CREATE POLICY "tenant_isolation" ON sites
  USING (customer_id = current_setting('app.customer_id'));

DROP POLICY IF EXISTS "tenant_isolation" ON captures;
CREATE POLICY "tenant_isolation" ON captures
  USING (customer_id = current_setting('app.customer_id'));
