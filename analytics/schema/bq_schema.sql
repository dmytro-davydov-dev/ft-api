-- Flowterra BigQuery Schema DDL
-- FLO-27 | Phase 4
-- Dataset: flowterra_{env}  (e.g. flowterra_dev, flowterra_prod)
-- Applied via: bq query --project_id={project} --use_legacy_sql=false < bq_schema.sql

-- ─────────────────────────────────────────────
-- location_events
-- Partition: DATE(ts)  |  Cluster: customerId, siteId
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `{project}.flowterra_{env}.location_events`
(
  ts          TIMESTAMP NOT NULL,  -- partition key (day granularity)
  customerId  STRING    NOT NULL,  -- cluster key #1 (tenant isolation)
  siteId      STRING    NOT NULL,  -- cluster key #2
  tagId       STRING    NOT NULL,
  areaId      STRING,
  gatewayId   STRING    NOT NULL,
  rssi        INT64,
  floor       INT64,
  batteryPct  INT64
)
PARTITION BY DATE(ts)
CLUSTER BY customerId, siteId
OPTIONS (partition_expiration_days = 365);

-- ─────────────────────────────────────────────
-- geofence_events
-- Partition: DATE(ts)  |  Cluster: customerId
-- ─────────────────────────────────────────────
CREATE TABLE IF NOT EXISTS `{project}.flowterra_{env}.geofence_events`
(
  ts          TIMESTAMP NOT NULL,  -- partition key (day)
  customerId  STRING    NOT NULL,  -- cluster key
  geofenceId  STRING    NOT NULL,
  tagId       STRING    NOT NULL,
  event       STRING    NOT NULL   -- 'enter' | 'exit'
)
PARTITION BY DATE(ts)
CLUSTER BY customerId
OPTIONS (partition_expiration_days = 365);
