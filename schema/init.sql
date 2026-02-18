PRAGMA foreign_keys = ON;

CREATE TABLE IF NOT EXISTS instances (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id TEXT NOT NULL UNIQUE,
  site_id TEXT NOT NULL UNIQUE,
  site_url TEXT,
  status TEXT NOT NULL CHECK (status IN ('active', 'pending_url', 'disabled')),
  pending_reason TEXT,
  site_pub_key TEXT NOT NULL,
  site_key_alg TEXT NOT NULL DEFAULT 'ed25519',
  minute_of_day INTEGER NOT NULL CHECK (minute_of_day >= 0 AND minute_of_day <= 1439),
  next_run_at TEXT,
  last_seen_at TEXT,
  last_success_at TEXT,
  app_version TEXT,
  build_id TEXT,
  commit_hash TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL
);

CREATE INDEX IF NOT EXISTS idx_instances_status_next_run ON instances(status, next_run_at);
CREATE INDEX IF NOT EXISTS idx_instances_last_seen ON instances(last_seen_at);

CREATE TABLE IF NOT EXISTS build_events (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id TEXT NOT NULL,
  built_at TEXT NOT NULL,
  app_version TEXT,
  build_id TEXT,
  commit_hash TEXT,
  idempotency_key TEXT NOT NULL UNIQUE,
  created_at TEXT NOT NULL,
  FOREIGN KEY (instance_id) REFERENCES instances(instance_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_build_events_instance_built_at ON build_events(instance_id, built_at DESC);

CREATE TABLE IF NOT EXISTS deliveries (
  id TEXT PRIMARY KEY,
  instance_id TEXT NOT NULL,
  scheduled_for TEXT NOT NULL,
  enqueued_at TEXT NOT NULL,
  status TEXT NOT NULL CHECK (status IN ('queued', 'delivered', 'failed', 'dead')),
  attempt_count INTEGER NOT NULL DEFAULT 0,
  response_status INTEGER,
  accepted INTEGER,
  dedup_hit INTEGER,
  last_error_code TEXT,
  last_error_message TEXT,
  completed_at TEXT,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  FOREIGN KEY (instance_id) REFERENCES instances(instance_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_deliveries_instance_created ON deliveries(instance_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_deliveries_status_created ON deliveries(status, created_at DESC);

CREATE TABLE IF NOT EXISTS delivery_attempts (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id TEXT NOT NULL,
  attempt_no INTEGER NOT NULL,
  started_at TEXT NOT NULL,
  ended_at TEXT NOT NULL,
  http_status INTEGER,
  timeout INTEGER,
  error_code TEXT,
  error_message TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (delivery_id) REFERENCES deliveries(id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_delivery_attempts_delivery_attempt ON delivery_attempts(delivery_id, attempt_no DESC);

CREATE TABLE IF NOT EXISTS dispatch_minute_load (
  minute_start TEXT PRIMARY KEY,
  scheduled_count INTEGER NOT NULL DEFAULT 0,
  retry_count INTEGER NOT NULL DEFAULT 0,
  total_count INTEGER NOT NULL DEFAULT 0,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  CHECK (total_count = scheduled_count + retry_count)
);

CREATE INDEX IF NOT EXISTS idx_dispatch_minute_load_updated_at ON dispatch_minute_load(updated_at DESC);

CREATE TABLE IF NOT EXISTS telemetry_samples (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  delivery_id TEXT NOT NULL UNIQUE,
  instance_id TEXT NOT NULL,
  collected_at TEXT NOT NULL,
  schema_ver TEXT,

  accepted INTEGER NOT NULL DEFAULT 0,
  dedup_hit INTEGER NOT NULL DEFAULT 0,
  verify_source TEXT,
  dnssec_ad INTEGER,
  verify_ms INTEGER,
  token_age_ms INTEGER,

  cron_enabled INTEGER,
  doctor_enabled INTEGER,
  projects_enabled INTEGER,
  friends_enabled INTEGER,

  latest_run_id INTEGER,
  latest_created_at TEXT,
  latest_status TEXT,
  latest_duration_ms INTEGER,
  enabled_count INTEGER,
  success_count INTEGER,
  failed_count INTEGER,
  skipped_count INTEGER,

  doctor_duration_ms INTEGER,
  projects_duration_ms INTEGER,
  friends_duration_ms INTEGER,

  health_record_id INTEGER,
  health_created_at TEXT,
  health_status TEXT,
  health_ok_count INTEGER,
  health_warning_count INTEGER,
  health_error_count INTEGER,
  db_latency_ms INTEGER,
  redis_latency_ms INTEGER,
  site_self_latency_ms INTEGER,

  app_version TEXT,
  runtime_node_version TEXT,
  build_id TEXT,
  commit_hash TEXT,

  raw_json TEXT,
  created_at TEXT NOT NULL,
  FOREIGN KEY (instance_id) REFERENCES instances(instance_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_telemetry_samples_instance_time ON telemetry_samples(instance_id, collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_samples_time ON telemetry_samples(collected_at DESC);
CREATE INDEX IF NOT EXISTS idx_telemetry_samples_status_time ON telemetry_samples(latest_status, collected_at DESC);

CREATE TABLE IF NOT EXISTS telemetry_hourly (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  instance_id TEXT NOT NULL,
  bucket_hour TEXT NOT NULL,
  total_samples INTEGER NOT NULL,
  accepted_count INTEGER NOT NULL,
  dedup_hit_count INTEGER NOT NULL,
  failed_count INTEGER NOT NULL,
  avg_latest_duration_ms REAL,
  max_latest_duration_ms INTEGER,
  created_at TEXT NOT NULL,
  updated_at TEXT NOT NULL,
  UNIQUE(instance_id, bucket_hour),
  FOREIGN KEY (instance_id) REFERENCES instances(instance_id) ON DELETE CASCADE
);

CREATE INDEX IF NOT EXISTS idx_telemetry_hourly_bucket ON telemetry_hourly(bucket_hour DESC);

CREATE TABLE IF NOT EXISTS cloud_signing_keys (
  kid TEXT PRIMARY KEY,
  alg TEXT NOT NULL,
  public_key TEXT NOT NULL,
  private_key_encrypted TEXT,
  status TEXT NOT NULL CHECK (status IN ('active', 'grace', 'retired')),
  created_at TEXT NOT NULL,
  retire_at TEXT
);
