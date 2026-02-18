import type { ParsedTelemetry } from "./types";
import { nowIso, toSqlBool, toSqlBoolOrNull, truncate } from "./utils";

export type DispatchSource = "scheduled" | "retry";

export async function createDelivery(input: {
  db: D1Database;
  deliveryId: string;
  instanceId: string;
  scheduledFor: string;
  enqueuedAt: string;
}): Promise<void> {
  await input.db.prepare(
    `INSERT INTO deliveries (
      id,
      instance_id,
      scheduled_for,
      enqueued_at,
      status,
      attempt_count,
      created_at,
      updated_at
    ) VALUES (?, ?, ?, ?, 'queued', 0, ?, ?)`,
  )
    .bind(
      input.deliveryId,
      input.instanceId,
      input.scheduledFor,
      input.enqueuedAt,
      input.enqueuedAt,
      input.enqueuedAt,
    )
    .run();
}

export async function markDeliveryDelivered(input: {
  db: D1Database;
  deliveryId: string;
  attemptCount: number;
  responseStatus: number | null;
  accepted: boolean;
  dedupHit: boolean;
}): Promise<void> {
  const now = nowIso();
  await input.db.prepare(
    `UPDATE deliveries
     SET status = 'delivered',
         attempt_count = ?,
         response_status = ?,
         accepted = ?,
         dedup_hit = ?,
         completed_at = ?,
         updated_at = ?
     WHERE id = ?`,
  )
    .bind(
      input.attemptCount,
      input.responseStatus,
      toSqlBool(input.accepted),
      toSqlBool(input.dedupHit),
      now,
      now,
      input.deliveryId,
    )
    .run();
}

export async function markDeliveryFailed(input: {
  db: D1Database;
  deliveryId: string;
  attemptCount: number;
  responseStatus: number | null;
  accepted: boolean;
  dedupHit: boolean;
  errorCode: string;
  errorMessage: string;
}): Promise<void> {
  await input.db.prepare(
    `UPDATE deliveries
     SET status = 'failed',
         attempt_count = ?,
         response_status = ?,
         accepted = ?,
         dedup_hit = ?,
         last_error_code = ?,
         last_error_message = ?,
         updated_at = ?
     WHERE id = ?`,
  )
    .bind(
      input.attemptCount,
      input.responseStatus,
      toSqlBool(input.accepted),
      toSqlBool(input.dedupHit),
      input.errorCode,
      truncate(input.errorMessage, 500),
      nowIso(),
      input.deliveryId,
    )
    .run();
}

export async function markDeliveryDead(input: {
  db: D1Database;
  deliveryId: string;
  errorCode: string;
  errorMessage: string;
}): Promise<void> {
  const now = nowIso();
  await input.db.prepare(
    `UPDATE deliveries
     SET status = 'dead',
         last_error_code = ?,
         last_error_message = ?,
         completed_at = ?,
         updated_at = ?
     WHERE id = ?`,
  )
    .bind(
      input.errorCode,
      truncate(input.errorMessage, 500),
      now,
      now,
      input.deliveryId,
    )
    .run();
}

export async function insertDeliveryAttempt(input: {
  db: D1Database;
  deliveryId: string;
  attemptNo: number;
  startedAt: string;
  endedAt: string;
  httpStatus: number | null;
  timeout: boolean;
  errorCode: string | null;
  errorMessage: string | null;
}): Promise<void> {
  await input.db.prepare(
    `INSERT INTO delivery_attempts (
      delivery_id,
      attempt_no,
      started_at,
      ended_at,
      http_status,
      timeout,
      error_code,
      error_message,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      input.deliveryId,
      input.attemptNo,
      input.startedAt,
      input.endedAt,
      input.httpStatus,
      toSqlBool(input.timeout),
      input.errorCode,
      truncate(input.errorMessage, 500),
      nowIso(),
    )
    .run();
}

export async function insertTelemetrySample(input: {
  db: D1Database;
  instanceId: string;
  deliveryId: string;
  telemetry: ParsedTelemetry;
}): Promise<void> {
  const telemetry = input.telemetry;

  await input.db.prepare(
    `INSERT OR IGNORE INTO telemetry_samples (
      delivery_id,
      instance_id,
      collected_at,
      schema_ver,
      accepted,
      dedup_hit,
      verify_source,
      dnssec_ad,
      verify_ms,
      token_age_ms,
      cron_enabled,
      doctor_enabled,
      projects_enabled,
      friends_enabled,
      latest_run_id,
      latest_created_at,
      latest_status,
      latest_duration_ms,
      enabled_count,
      success_count,
      failed_count,
      skipped_count,
      doctor_duration_ms,
      projects_duration_ms,
      friends_duration_ms,
      health_record_id,
      health_created_at,
      health_status,
      health_ok_count,
      health_warning_count,
      health_error_count,
      db_latency_ms,
      redis_latency_ms,
      site_self_latency_ms,
      app_version,
      runtime_node_version,
      build_id,
      commit_hash,
      raw_json,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      input.deliveryId,
      input.instanceId,
      telemetry.collectedAt,
      telemetry.schemaVer,
      toSqlBool(telemetry.accepted),
      toSqlBool(telemetry.dedupHit),
      telemetry.verifySource,
      toSqlBoolOrNull(telemetry.dnssecAd),
      telemetry.verifyMs,
      telemetry.tokenAgeMs,
      toSqlBoolOrNull(telemetry.cronEnabled),
      toSqlBoolOrNull(telemetry.doctorEnabled),
      toSqlBoolOrNull(telemetry.projectsEnabled),
      toSqlBoolOrNull(telemetry.friendsEnabled),
      telemetry.latestRunId,
      telemetry.latestCreatedAt,
      telemetry.latestStatus,
      telemetry.latestDurationMs,
      telemetry.enabledCount,
      telemetry.successCount,
      telemetry.failedCount,
      telemetry.skippedCount,
      telemetry.doctorDurationMs,
      telemetry.projectsDurationMs,
      telemetry.friendsDurationMs,
      telemetry.healthRecordId,
      telemetry.healthCreatedAt,
      telemetry.healthStatus,
      telemetry.healthOkCount,
      telemetry.healthWarningCount,
      telemetry.healthErrorCount,
      telemetry.dbLatencyMs,
      telemetry.redisLatencyMs,
      telemetry.siteSelfLatencyMs,
      telemetry.appVersion,
      telemetry.runtimeNodeVersion,
      telemetry.buildId,
      telemetry.commitHash,
      telemetry.rawJson,
      nowIso(),
    )
    .run();
}

function floorToMinute(date: Date): Date {
  return new Date(
    Date.UTC(
      date.getUTCFullYear(),
      date.getUTCMonth(),
      date.getUTCDate(),
      date.getUTCHours(),
      date.getUTCMinutes(),
      0,
      0,
    ),
  );
}

function normalizePreferredDate(preferredAt: Date | string): Date {
  const date =
    typeof preferredAt === "string" ? new Date(preferredAt) : preferredAt;
  if (Number.isNaN(date.getTime())) {
    return floorToMinute(new Date());
  }
  return floorToMinute(date);
}

export async function reserveDispatchSlot(input: {
  db: D1Database;
  preferredAt: Date | string;
  source: DispatchSource;
  maxPerMinute: number;
  lookaheadMinutes: number;
}): Promise<{
  minuteStart: string;
  offsetMinutes: number;
  totalCount: number;
  scheduledCount: number;
  retryCount: number;
} | null> {
  const baseMinute = normalizePreferredDate(input.preferredAt);
  const scheduledInc = input.source === "scheduled" ? 1 : 0;
  const retryInc = input.source === "retry" ? 1 : 0;

  for (let offset = 0; offset <= input.lookaheadMinutes; offset += 1) {
    const minuteDate = new Date(baseMinute.getTime() + offset * 60_000);
    const minuteStart = minuteDate.toISOString();
    const now = nowIso();

    const row = await input.db
      .prepare(
        `INSERT INTO dispatch_minute_load (
          minute_start,
          scheduled_count,
          retry_count,
          total_count,
          created_at,
          updated_at
        ) VALUES (?, ?, ?, 1, ?, ?)
        ON CONFLICT(minute_start) DO UPDATE SET
          scheduled_count = dispatch_minute_load.scheduled_count + excluded.scheduled_count,
          retry_count = dispatch_minute_load.retry_count + excluded.retry_count,
          total_count = dispatch_minute_load.total_count + 1,
          updated_at = excluded.updated_at
        WHERE dispatch_minute_load.total_count < ?
        RETURNING
          minute_start,
          scheduled_count,
          retry_count,
          total_count`,
      )
      .bind(
        minuteStart,
        scheduledInc,
        retryInc,
        now,
        now,
        input.maxPerMinute,
      )
      .first<{
        minute_start: string;
        total_count: number;
        scheduled_count: number;
        retry_count: number;
      }>();

    if (!row) {
      continue;
    }

    return {
      minuteStart: row.minute_start,
      offsetMinutes: offset,
      totalCount: row.total_count,
      scheduledCount: row.scheduled_count,
      retryCount: row.retry_count,
    };
  }

  return null;
}

export async function cleanupDispatchMinuteLoad(input: {
  db: D1Database;
  retainDays: number;
}): Promise<void> {
  const cutoff = new Date(
    Date.now() - Math.max(1, input.retainDays) * 24 * 60 * 60 * 1000,
  ).toISOString();

  await input.db
    .prepare(`DELETE FROM dispatch_minute_load WHERE minute_start < ?`)
    .bind(cutoff)
    .run();
}
