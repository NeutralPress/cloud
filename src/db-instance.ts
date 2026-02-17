
import type { DueInstanceRow, InstanceRow, InstanceStatus } from "./types";
import {
  computeNextRunAt,
  generateInstanceId,
  normalizeNullableString,
  nowIso,
  randomMinuteOfDay,
} from "./utils";

export async function getInstanceBySiteId(
  db: D1Database,
  siteId: string,
): Promise<InstanceRow | null> {
  const row = await db
    .prepare(
      `SELECT
        instance_id,
        site_id,
        site_url,
        status,
        pending_reason,
        site_pub_key,
        site_key_alg,
        minute_of_day,
        next_run_at
      FROM instances
      WHERE site_id = ?
      LIMIT 1`,
    )
    .bind(siteId)
    .first<InstanceRow>();

  return row ?? null;
}

export async function getInstanceByInstanceId(
  db: D1Database,
  instanceId: string,
): Promise<InstanceRow | null> {
  const row = await db
    .prepare(
      `SELECT
        instance_id,
        site_id,
        site_url,
        status,
        pending_reason,
        site_pub_key,
        site_key_alg,
        minute_of_day,
        next_run_at
      FROM instances
      WHERE instance_id = ?
      LIMIT 1`,
    )
    .bind(instanceId)
    .first<InstanceRow>();

  return row ?? null;
}

export async function upsertInstance(input: {
  db: D1Database;
  existing: InstanceRow | null;
  siteId: string;
  sitePubKey: string;
  siteKeyAlg: string;
  normalizedSiteUrl: string | null;
  pendingReason: string | null;
  appVersion: string | null;
  buildId: string | null;
  commitHash: string | null;
}): Promise<{
  instanceId: string;
  minuteOfDay: number;
  status: InstanceStatus;
  nextRunAt: string | null;
}> {
  const now = nowIso();
  const status: InstanceStatus = input.pendingReason ? "pending_url" : "active";

  if (!input.existing) {
    const instanceId = generateInstanceId();
    const minuteOfDay = randomMinuteOfDay();
    const nextRunAt = status === "active" ? computeNextRunAt(minuteOfDay, new Date()) : null;

    await input.db.prepare(
      `INSERT INTO instances (
        instance_id,
        site_id,
        site_url,
        status,
        pending_reason,
        site_pub_key,
        site_key_alg,
        minute_of_day,
        next_run_at,
        last_seen_at,
        app_version,
        build_id,
        commit_hash,
        created_at,
        updated_at
      ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`,
    )
      .bind(
        instanceId,
        input.siteId,
        input.normalizedSiteUrl,
        status,
        input.pendingReason,
        input.sitePubKey,
        input.siteKeyAlg,
        minuteOfDay,
        nextRunAt,
        now,
        input.appVersion,
        input.buildId,
        input.commitHash,
        now,
        now,
      )
      .run();

    return { instanceId, minuteOfDay, status, nextRunAt };
  }

  const instanceId = input.existing.instance_id;
  const minuteOfDay = input.existing.minute_of_day;
  const nextRunAt = status === "active" ? computeNextRunAt(minuteOfDay, new Date()) : null;

  await input.db.prepare(
    `UPDATE instances
    SET
      site_url = ?,
      status = ?,
      pending_reason = ?,
      site_pub_key = ?,
      site_key_alg = ?,
      minute_of_day = ?,
      next_run_at = ?,
      last_seen_at = ?,
      app_version = ?,
      build_id = ?,
      commit_hash = ?,
      updated_at = ?
    WHERE site_id = ?`,
  )
    .bind(
      input.normalizedSiteUrl,
      status,
      input.pendingReason,
      input.existing.site_pub_key,
      input.siteKeyAlg,
      minuteOfDay,
      nextRunAt,
      now,
      input.appVersion,
      input.buildId,
      input.commitHash,
      now,
      input.siteId,
    )
    .run();

  return { instanceId, minuteOfDay, status, nextRunAt };
}

export async function disableInstance(input: {
  db: D1Database;
  siteId: string;
  reason: string | null;
}): Promise<void> {
  const now = nowIso();
  await input.db.prepare(
    `UPDATE instances
    SET status = 'disabled',
        pending_reason = ?,
        next_run_at = NULL,
        updated_at = ?
    WHERE site_id = ?`,
  )
    .bind(input.reason ?? "deregistered", now, input.siteId)
    .run();
}

export async function insertBuildEvent(input: {
  db: D1Database;
  instanceId: string;
  builtAt: string;
  appVersion: string | null;
  buildId: string | null;
  commitHash: string | null;
  idempotencyKey: string;
}): Promise<void> {
  await input.db.prepare(
    `INSERT OR IGNORE INTO build_events (
      instance_id,
      built_at,
      app_version,
      build_id,
      commit_hash,
      idempotency_key,
      created_at
    ) VALUES (?, ?, ?, ?, ?, ?, ?)`,
  )
    .bind(
      input.instanceId,
      input.builtAt,
      input.appVersion,
      input.buildId,
      input.commitHash,
      input.idempotencyKey,
      nowIso(),
    )
    .run();
}

export async function getDueInstances(
  db: D1Database,
  now: string,
  limit: number,
): Promise<DueInstanceRow[]> {
  const rows = await db.prepare(
    `SELECT instance_id, site_id, site_url, minute_of_day, next_run_at
     FROM instances
     WHERE status = 'active'
       AND pending_reason IS NULL
       AND site_url IS NOT NULL
       AND next_run_at IS NOT NULL
       AND next_run_at <= ?
     ORDER BY next_run_at ASC
     LIMIT ?`,
  )
    .bind(now, limit)
    .all<DueInstanceRow>();

  return rows.results ?? [];
}

export async function updateInstanceNextRun(
  db: D1Database,
  instanceId: string,
  nextRunAt: string,
): Promise<void> {
  const now = nowIso();
  await db.prepare(
    `UPDATE instances SET next_run_at = ?, updated_at = ? WHERE instance_id = ?`,
  )
    .bind(nextRunAt, now, instanceId)
    .run();
}

export async function updateInstanceLastSuccess(
  db: D1Database,
  instanceId: string,
): Promise<void> {
  const now = nowIso();
  await db.prepare(
    `UPDATE instances SET last_success_at = ?, updated_at = ? WHERE instance_id = ?`,
  )
    .bind(now, now, instanceId)
    .run();
}

export async function getHealthSummary(db: D1Database): Promise<{
  instances: { active: number; pending: number; disabled: number };
  deliveries: { queued: number; failed: number; dead: number; delivered: number };
}> {
  const [instanceSummary, deliverySummary] = await Promise.all([
    db.prepare(
      `SELECT
        SUM(CASE WHEN status = 'active' THEN 1 ELSE 0 END) AS active,
        SUM(CASE WHEN status = 'pending_url' THEN 1 ELSE 0 END) AS pending,
        SUM(CASE WHEN status = 'disabled' THEN 1 ELSE 0 END) AS disabled
      FROM instances`,
    ).first<{ active: number | null; pending: number | null; disabled: number | null }>(),
    db.prepare(
      `SELECT
        SUM(CASE WHEN status = 'queued' THEN 1 ELSE 0 END) AS queued,
        SUM(CASE WHEN status = 'failed' THEN 1 ELSE 0 END) AS failed,
        SUM(CASE WHEN status = 'dead' THEN 1 ELSE 0 END) AS dead,
        SUM(CASE WHEN status = 'delivered' THEN 1 ELSE 0 END) AS delivered
      FROM deliveries
      WHERE created_at >= ?`,
    )
      .bind(new Date(Date.now() - 24 * 60 * 60 * 1000).toISOString())
      .first<{ queued: number | null; failed: number | null; dead: number | null; delivered: number | null }>(),
  ]);

  return {
    instances: {
      active: instanceSummary?.active ?? 0,
      pending: instanceSummary?.pending ?? 0,
      disabled: instanceSummary?.disabled ?? 0,
    },
    deliveries: {
      queued: deliverySummary?.queued ?? 0,
      failed: deliverySummary?.failed ?? 0,
      dead: deliverySummary?.dead ?? 0,
      delivered: deliverySummary?.delivered ?? 0,
    },
  };
}

export async function runMaintenance(db: D1Database): Promise<void> {
  const now = nowIso();
  const cutoff90d = new Date(Date.now() - 90 * 24 * 60 * 60 * 1000).toISOString();
  const cutoff365d = new Date(Date.now() - 365 * 24 * 60 * 60 * 1000).toISOString();
  const aggregateSince = new Date(Date.now() - 2 * 60 * 60 * 1000).toISOString();

  await db.prepare(`DELETE FROM telemetry_samples WHERE collected_at < ?`).bind(cutoff90d).run();
  await db.prepare(`DELETE FROM telemetry_hourly WHERE bucket_hour < ?`).bind(cutoff365d).run();
  await db.prepare(`DELETE FROM build_events WHERE built_at < ?`).bind(cutoff365d).run();

  await db.prepare(
    `INSERT INTO telemetry_hourly (
      instance_id,
      bucket_hour,
      total_samples,
      accepted_count,
      dedup_hit_count,
      failed_count,
      avg_latest_duration_ms,
      max_latest_duration_ms,
      created_at,
      updated_at
    )
    SELECT
      instance_id,
      substr(collected_at, 1, 13) || ':00:00.000Z' AS bucket_hour,
      COUNT(*) AS total_samples,
      SUM(CASE WHEN accepted = 1 THEN 1 ELSE 0 END) AS accepted_count,
      SUM(CASE WHEN dedup_hit = 1 THEN 1 ELSE 0 END) AS dedup_hit_count,
      SUM(CASE WHEN latest_status IS NOT NULL AND latest_status != 'OK' THEN 1 ELSE 0 END) AS failed_count,
      AVG(COALESCE(latest_duration_ms, 0)) AS avg_latest_duration_ms,
      MAX(COALESCE(latest_duration_ms, 0)) AS max_latest_duration_ms,
      ?,
      ?
    FROM telemetry_samples
    WHERE collected_at >= ?
    GROUP BY instance_id, bucket_hour
    ON CONFLICT(instance_id, bucket_hour) DO UPDATE SET
      total_samples = excluded.total_samples,
      accepted_count = excluded.accepted_count,
      dedup_hit_count = excluded.dedup_hit_count,
      failed_count = excluded.failed_count,
      avg_latest_duration_ms = excluded.avg_latest_duration_ms,
      max_latest_duration_ms = excluded.max_latest_duration_ms,
      updated_at = excluded.updated_at`,
  )
    .bind(now, now, aggregateSince)
    .run();
}
