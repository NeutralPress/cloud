export type InstanceStatus = "active" | "pending_url" | "disabled";
export type QueueResult = "success" | "retry" | "drop";

export interface EnvBindings {
  DB: D1Database;
  DISPATCH_QUEUE: Queue<DispatchMessage>;
  CLOUD_JWKS_JSON: string;
  CLOUD_PRIVATE_KEYS_JSON: string;
  CLOUD_ACTIVE_KID?: string;
  CLOUD_ISSUER?: string;
  INSTANCE_TRIGGER_AUDIENCE?: string;
  INSTANCE_TRIGGER_PATH?: string;
  REQUEST_TIMEOUT_MS?: string;
  MAX_RETRY_ATTEMPTS?: string;
  MAX_DISPATCH_PER_MINUTE?: string;
  TELEMETRY_RAW_MAX_BYTES?: string;
  LOG_LEVEL?: string;
}

export interface InstanceRow {
  instance_id: string;
  site_id: string;
  site_url: string | null;
  status: InstanceStatus;
  pending_reason: string | null;
  site_pub_key: string;
  site_key_alg: string;
  minute_of_day: number;
  next_run_at: string | null;
}

export interface DueInstanceRow {
  instance_id: string;
  site_id: string;
  site_url: string;
  minute_of_day: number;
  next_run_at: string;
}

export interface DispatchMessage {
  deliveryId: string;
  instanceId: string;
  siteId: string;
  siteUrl: string;
  scheduledFor: string;
  enqueuedAt: string;
  dispatchAttempt: number;
}

export interface ParsedTelemetry {
  schemaVer: string | null;
  collectedAt: string;
  accepted: boolean;
  dedupHit: boolean;
  verifySource: string | null;
  dnssecAd: boolean | null;
  verifyMs: number | null;
  tokenAgeMs: number | null;
  cronEnabled: boolean | null;
  doctorEnabled: boolean | null;
  projectsEnabled: boolean | null;
  friendsEnabled: boolean | null;
  latestRunId: number | null;
  latestCreatedAt: string | null;
  latestStatus: string | null;
  latestDurationMs: number | null;
  enabledCount: number | null;
  successCount: number | null;
  failedCount: number | null;
  skippedCount: number | null;
  doctorDurationMs: number | null;
  projectsDurationMs: number | null;
  friendsDurationMs: number | null;
  healthRecordId: number | null;
  healthCreatedAt: string | null;
  healthStatus: string | null;
  healthOkCount: number | null;
  healthWarningCount: number | null;
  healthErrorCount: number | null;
  dbLatencyMs: number | null;
  redisLatencyMs: number | null;
  siteSelfLatencyMs: number | null;
  appVersion: string | null;
  runtimeNodeVersion: string | null;
  buildId: string | null;
  commitHash: string | null;
  rawJson: string | null;
}
