import {
  DEFAULT_MAX_DISPATCH_PER_MINUTE,
  MAX_SCHEDULE_SCAN_PER_TICK,
  MAX_SLOT_LOOKAHEAD_MINUTES,
  SCHEDULE_BATCH_LIMIT,
} from "./constants";
import {
  reserveDispatchSlot,
  createDelivery,
  insertDeliveryAttempt,
  insertTelemetrySample,
  markDeliveryDead,
  markDeliveryDelivered,
  markDeliveryFailed,
} from "./db-delivery";
import {
  getDueInstances,
  getInstanceByInstanceId,
  runMaintenance,
  updateInstanceLastSuccess,
  updateInstanceNextRun,
} from "./db-instance";
import { logDebug, logError, logInfo, logWarn } from "./logger";
import { dispatchMessageSchema } from "./schemas";
import { signCloudTriggerToken } from "./security";
import { parseTelemetry } from "./telemetry";
import type { DispatchMessage, EnvBindings, QueueResult } from "./types";
import {
  computeNextRunAt,
  errorToMessage,
  fetchWithTimeout,
  generateDeliveryId,
  joinUrl,
  nowIso,
  parsePositiveInt,
  safeJsonParse,
  truncate,
} from "./utils";

export async function handleScheduled(
  controller: ScheduledController,
  env: EnvBindings,
): Promise<void> {
  const nowDate = new Date(controller.scheduledTime);
  const now = nowIso();
  const tickStartMs = Date.now();
  const maxDispatchPerMinute = parsePositiveInt(
    env.MAX_DISPATCH_PER_MINUTE,
    DEFAULT_MAX_DISPATCH_PER_MINUTE,
  );

  let totalEnqueued = 0;
  let totalFetched = 0;
  let batchCount = 0;
  let queueSendFailed = 0;

  logInfo(env, "cron.schedule.tick_start", {
    scheduledTime: nowDate.toISOString(),
    maxScheduleScanPerTick: MAX_SCHEDULE_SCAN_PER_TICK,
    scheduleBatchLimit: SCHEDULE_BATCH_LIMIT,
    maxDispatchPerMinute,
  });

  while (totalEnqueued < MAX_SCHEDULE_SCAN_PER_TICK) {
    const rows = await getDueInstances(env.DB, now, SCHEDULE_BATCH_LIMIT);
    if (rows.length === 0) break;
    batchCount += 1;
    totalFetched += rows.length;

    logDebug(env, "cron.schedule.batch_fetched", {
      batchIndex: batchCount,
      fetchedCount: rows.length,
      totalFetched,
      totalEnqueued,
    });

    for (const row of rows) {
      if (totalEnqueued >= MAX_SCHEDULE_SCAN_PER_TICK) break;

      const reservedSlot = await reserveDispatchSlot({
        db: env.DB,
        preferredAt: nowDate,
        source: "scheduled",
        maxPerMinute: maxDispatchPerMinute,
        lookaheadMinutes: MAX_SLOT_LOOKAHEAD_MINUTES,
      });

      if (!reservedSlot) {
        queueSendFailed += 1;

        logError(env, "cron.schedule.slot_reserve_failed", {
          instanceId: row.instance_id,
          siteId: row.site_id,
          scheduledFor: row.next_run_at,
          maxDispatchPerMinute,
          lookaheadMinutes: MAX_SLOT_LOOKAHEAD_MINUTES,
        });

        continue;
      }

      const deliveryId = generateDeliveryId();
      const enqueuedAt = nowIso();
      const message: DispatchMessage = {
        deliveryId,
        instanceId: row.instance_id,
        siteId: row.site_id,
        siteUrl: row.site_url,
        scheduledFor: row.next_run_at,
        enqueuedAt,
        dispatchAttempt: 1,
      };

      await createDelivery({
        db: env.DB,
        deliveryId,
        instanceId: row.instance_id,
        scheduledFor: row.next_run_at,
        enqueuedAt,
      });

      try {
        await sendToDispatchQueue(env, message, reservedSlot.minuteStart);
      } catch (error) {
        queueSendFailed += 1;
        await markDeliveryFailed({
          db: env.DB,
          deliveryId,
          attemptCount: 0,
          responseStatus: null,
          accepted: false,
          dedupHit: false,
          errorCode: "QUEUE_SEND_FAILED",
          errorMessage: truncate(errorToMessage(error, "队列发送失败"), 500) ?? "队列发送失败",
        });

        await markDeliveryDead({
          db: env.DB,
          deliveryId,
          errorCode: "QUEUE_SEND_FAILED",
          errorMessage: "消息未能写入队列，已标记 dead",
        });

        logError(env, "cron.schedule.enqueue_failed", {
          deliveryId,
          instanceId: row.instance_id,
          siteId: row.site_id,
          errorMessage: errorToMessage(error, "队列发送失败"),
        });

        continue;
      }

      const nextRunAt = computeNextRunAt(row.minute_of_day, nowDate);

      await updateInstanceNextRun(env.DB, row.instance_id, nextRunAt);

      totalEnqueued += 1;
      const delaySeconds = computeDelaySeconds(reservedSlot.minuteStart);

      logDebug(env, "cron.schedule.enqueued", {
        deliveryId,
        instanceId: row.instance_id,
        siteId: row.site_id,
        scheduledFor: row.next_run_at,
        nextRunAt,
        queueMinute: reservedSlot.minuteStart,
        slotOffsetMinutes: reservedSlot.offsetMinutes,
        slotTotalCount: reservedSlot.totalCount,
        delaySeconds,
        totalEnqueued,
      });
    }
  }

  if (nowDate.getUTCMinutes() === 13) {
    await runMaintenance(env.DB);
    logInfo(env, "cron.schedule.maintenance_done", {
      scheduledTime: nowDate.toISOString(),
    });
  }

  logInfo(env, "cron.schedule.tick_end", {
    scheduledTime: nowDate.toISOString(),
    totalEnqueued,
    totalFetched,
    batchCount,
    queueSendFailed,
    durationMs: Date.now() - tickStartMs,
  });
}

export async function handleQueue(
  batch: MessageBatch<DispatchMessage>,
  env: EnvBindings,
): Promise<void> {
  const batchStartMs = Date.now();
  let ackedCount = 0;
  let retryCount = 0;
  let invalidCount = 0;
  let deadCount = 0;
  let successCount = 0;
  let dropCount = 0;

  logInfo(env, "cron.queue.batch_start", {
    queue: batch.queue,
    messageCount: batch.messages.length,
  });

  if (batch.queue.endsWith("-dlq")) {
    await handleDlq(batch, env);
    logInfo(env, "cron.queue.batch_end", {
      queue: batch.queue,
      messageCount: batch.messages.length,
      durationMs: Date.now() - batchStartMs,
      dlq: true,
    });
    return;
  }

  const maxAttempts = parsePositiveInt(env.MAX_RETRY_ATTEMPTS, 6);
  const maxDispatchPerMinute = parsePositiveInt(
    env.MAX_DISPATCH_PER_MINUTE,
    DEFAULT_MAX_DISPATCH_PER_MINUTE,
  );

  for (const message of batch.messages) {
    const parsed = dispatchMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      invalidCount += 1;
      ackedCount += 1;
      logWarn(env, "cron.queue.invalid_payload", {
        queue: batch.queue,
        attempts: message.attempts || 1,
        error: parsed.error.flatten(),
      });
      message.ack();
      continue;
    }

    const attemptNo = parsed.data.dispatchAttempt;
    const result = await dispatchToInstance(parsed.data, env, attemptNo);

    if (result === "success" || result === "drop") {
      ackedCount += 1;
      if (result === "success") {
        successCount += 1;
      } else {
        dropCount += 1;
      }
      message.ack();
      continue;
    }

    if (attemptNo >= maxAttempts) {
      await markDeliveryDead({
        db: env.DB,
        deliveryId: parsed.data.deliveryId,
        errorCode: "MAX_ATTEMPTS_EXCEEDED",
        errorMessage: `投递重试次数超过上限: ${attemptNo}`,
      });
      deadCount += 1;
      ackedCount += 1;
      logWarn(env, "cron.queue.max_attempts_reached", {
        queue: batch.queue,
        deliveryId: parsed.data.deliveryId,
        attemptNo,
        maxAttempts,
      });
      message.ack();
      continue;
    }

    const retrySchedule = await scheduleRetryDispatch({
      env,
      message: parsed.data,
      currentAttemptNo: attemptNo,
      maxDispatchPerMinute,
    });

    if (!retrySchedule.ok) {
      await markDeliveryDead({
        db: env.DB,
        deliveryId: parsed.data.deliveryId,
        errorCode: "RETRY_SCHEDULE_FAILED",
        errorMessage: retrySchedule.reason ?? "重试调度失败",
      });

      deadCount += 1;
      ackedCount += 1;
      logError(env, "cron.queue.retry_schedule_failed", {
        queue: batch.queue,
        deliveryId: parsed.data.deliveryId,
        attemptNo,
        maxAttempts,
        reason: retrySchedule.reason ?? "unknown",
      });

      message.ack();
      continue;
    }

    retryCount += 1;
    ackedCount += 1;
    message.ack();
  }

  logInfo(env, "cron.queue.batch_end", {
    queue: batch.queue,
    messageCount: batch.messages.length,
    ackedCount,
    retryCount,
    invalidCount,
    deadCount,
    successCount,
    dropCount,
    durationMs: Date.now() - batchStartMs,
    dlq: false,
  });
}

async function handleDlq(
  batch: MessageBatch<DispatchMessage>,
  env: EnvBindings,
): Promise<void> {
  let ackedCount = 0;
  let invalidCount = 0;

  logWarn(env, "cron.dlq.batch_start", {
    queue: batch.queue,
    messageCount: batch.messages.length,
  });

  for (const message of batch.messages) {
    const parsed = dispatchMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      invalidCount += 1;
      ackedCount += 1;
      logWarn(env, "cron.dlq.invalid_payload", {
        queue: batch.queue,
        error: parsed.error.flatten(),
      });
      message.ack();
      continue;
    }

    await markDeliveryDead({
      db: env.DB,
      deliveryId: parsed.data.deliveryId,
      errorCode: "DLQ_REACHED",
      errorMessage: "消息进入死信队列",
    });

    ackedCount += 1;
    logWarn(env, "cron.dlq.marked_dead", {
      queue: batch.queue,
      deliveryId: parsed.data.deliveryId,
      siteId: parsed.data.siteId,
      instanceId: parsed.data.instanceId,
    });
    message.ack();
  }

  logWarn(env, "cron.dlq.batch_end", {
    queue: batch.queue,
    messageCount: batch.messages.length,
    ackedCount,
    invalidCount,
  });
}

function computeDelaySeconds(minuteStartIso: string): number {
  const targetMs = Date.parse(minuteStartIso);
  if (!Number.isFinite(targetMs)) return 0;
  return Math.max(0, Math.ceil((targetMs - Date.now()) / 1000));
}

function computeRetryBackoffMs(currentAttemptNo: number): number {
  const cappedAttempt = Math.max(1, currentAttemptNo);
  const backoffSeconds = Math.min(30 * 2 ** (cappedAttempt - 1), 15 * 60);
  return backoffSeconds * 1000;
}

async function sendToDispatchQueue(
  env: EnvBindings,
  message: DispatchMessage,
  minuteStartIso: string,
): Promise<void> {
  const delaySeconds = computeDelaySeconds(minuteStartIso);
  if (delaySeconds > 0) {
    await env.DISPATCH_QUEUE.send(message, { delaySeconds });
    return;
  }

  await env.DISPATCH_QUEUE.send(message);
}

async function scheduleRetryDispatch(input: {
  env: EnvBindings;
  message: DispatchMessage;
  currentAttemptNo: number;
  maxDispatchPerMinute: number;
}): Promise<{ ok: true } | { ok: false; reason: string }> {
  const nextAttemptNo = input.currentAttemptNo + 1;
  const preferredAt = new Date(Date.now() + computeRetryBackoffMs(input.currentAttemptNo));

  const reservedSlot = await reserveDispatchSlot({
    db: input.env.DB,
    preferredAt,
    source: "retry",
    maxPerMinute: input.maxDispatchPerMinute,
    lookaheadMinutes: MAX_SLOT_LOOKAHEAD_MINUTES,
  });

  if (!reservedSlot) {
    return {
      ok: false,
      reason: "重试队列容量不足，无法分配分钟配额",
    };
  }

  const retryMessage: DispatchMessage = {
    ...input.message,
    dispatchAttempt: nextAttemptNo,
    enqueuedAt: nowIso(),
  };

  try {
    await sendToDispatchQueue(input.env, retryMessage, reservedSlot.minuteStart);

    logWarn(input.env, "cron.queue.retry_scheduled", {
      deliveryId: input.message.deliveryId,
      siteId: input.message.siteId,
      currentAttemptNo: input.currentAttemptNo,
      nextAttemptNo,
      queueMinute: reservedSlot.minuteStart,
      slotOffsetMinutes: reservedSlot.offsetMinutes,
      slotTotalCount: reservedSlot.totalCount,
      slotRetryCount: reservedSlot.retryCount,
      slotScheduledCount: reservedSlot.scheduledCount,
      delaySeconds: computeDelaySeconds(reservedSlot.minuteStart),
    });

    return { ok: true };
  } catch (error) {
    return {
      ok: false,
      reason: `重试消息入队失败: ${errorToMessage(error, "queue send failed")}`,
    };
  }
}

async function dispatchToInstance(
  message: DispatchMessage,
  env: EnvBindings,
  attemptNo: number,
): Promise<QueueResult> {
  const dispatchStartMs = Date.now();
  const startedAt = nowIso();
  const requestTimeoutMs = parsePositiveInt(env.REQUEST_TIMEOUT_MS, 15000);
  const triggerPath = env.INSTANCE_TRIGGER_PATH || "/api/internal/cron/cloud-trigger";

  logDebug(env, "cron.dispatch.start", {
    deliveryId: message.deliveryId,
    instanceId: message.instanceId,
    siteId: message.siteId,
    attemptNo,
    scheduledFor: message.scheduledFor,
    enqueuedAt: message.enqueuedAt,
  });

  const instance = await getInstanceByInstanceId(env.DB, message.instanceId);
  if (!instance || instance.status !== "active" || !instance.site_url) {
    await insertDeliveryAttempt({
      db: env.DB,
      deliveryId: message.deliveryId,
      attemptNo,
      startedAt,
      endedAt: nowIso(),
      httpStatus: null,
      timeout: false,
      errorCode: "INSTANCE_NOT_ACTIVE",
      errorMessage: "实例不存在或未处于可触发状态",
    });

    await markDeliveryDead({
      db: env.DB,
      deliveryId: message.deliveryId,
      errorCode: "INSTANCE_NOT_ACTIVE",
      errorMessage: "实例不存在或未处于可触发状态",
    });

    logWarn(env, "cron.dispatch.instance_not_active", {
      deliveryId: message.deliveryId,
      instanceId: message.instanceId,
      siteId: message.siteId,
      attemptNo,
      instanceStatus: instance?.status ?? "missing",
      durationMs: Date.now() - dispatchStartMs,
    });

    return "drop";
  }

  let token: string;
  try {
    token = await signCloudTriggerToken(env, {
      deliveryId: message.deliveryId,
      siteId: instance.site_id,
    });
  } catch (error) {
    const errorMessage = truncate(errorToMessage(error, "生成签名令牌失败"), 500) ?? "生成签名令牌失败";

    await insertDeliveryAttempt({
      db: env.DB,
      deliveryId: message.deliveryId,
      attemptNo,
      startedAt,
      endedAt: nowIso(),
      httpStatus: null,
      timeout: false,
      errorCode: "TOKEN_SIGN_FAILED",
      errorMessage,
    });

    await markDeliveryFailed({
      db: env.DB,
      deliveryId: message.deliveryId,
      attemptCount: attemptNo,
      responseStatus: null,
      accepted: false,
      dedupHit: false,
      errorCode: "TOKEN_SIGN_FAILED",
      errorMessage,
    });

    logError(env, "cron.dispatch.token_sign_failed", {
      deliveryId: message.deliveryId,
      instanceId: message.instanceId,
      siteId: message.siteId,
      attemptNo,
      errorMessage,
      durationMs: Date.now() - dispatchStartMs,
    });

    return "retry";
  }

  const triggerUrl = joinUrl(instance.site_url, triggerPath);

  let httpStatus: number | null = null;
  let timeout = false;
  let errorCode: string | null = null;
  let errorMessage: string | null = null;
  let accepted = false;
  let dedupHit = false;

  try {
    const response = await fetchWithTimeout(
      triggerUrl,
      {
        method: "POST",
        headers: {
          authorization: `Bearer ${token}`,
          "content-type": "application/json",
          "x-np-delivery-id": message.deliveryId,
          "x-np-site-id": instance.site_id,
        },
        body: JSON.stringify({
          deliveryId: message.deliveryId,
          siteId: instance.site_id,
          triggerType: "CLOUD",
          requestedAt: nowIso(),
        }),
      },
      requestTimeoutMs,
    );

    httpStatus = response.status;
    const rawText = await response.text();
    const telemetry = parseTelemetry(safeJsonParse(rawText), {
      fallbackCollectedAt: nowIso(),
      rawMaxBytes: parsePositiveInt(env.TELEMETRY_RAW_MAX_BYTES, 4096),
      rawText,
    });

    accepted = telemetry.accepted;
    dedupHit = telemetry.dedupHit;

    await insertDeliveryAttempt({
      db: env.DB,
      deliveryId: message.deliveryId,
      attemptNo,
      startedAt,
      endedAt: nowIso(),
      httpStatus,
      timeout: false,
      errorCode: null,
      errorMessage: null,
    });

    if (response.ok && accepted) {
      await markDeliveryDelivered({
        db: env.DB,
        deliveryId: message.deliveryId,
        attemptCount: attemptNo,
        responseStatus: httpStatus,
        accepted,
        dedupHit,
      });

      await insertTelemetrySample({
        db: env.DB,
        instanceId: message.instanceId,
        deliveryId: message.deliveryId,
        telemetry,
      });

      await updateInstanceLastSuccess(env.DB, message.instanceId);

      logInfo(env, "cron.dispatch.success", {
        deliveryId: message.deliveryId,
        instanceId: message.instanceId,
        siteId: message.siteId,
        attemptNo,
        httpStatus,
        accepted,
        dedupHit,
        latestStatus: telemetry.latestStatus,
        latestDurationMs: telemetry.latestDurationMs,
        appVersion: telemetry.appVersion,
        durationMs: Date.now() - dispatchStartMs,
      });
      return "success";
    }

    errorCode = "UNACCEPTED_RESPONSE";
    errorMessage = truncate(`HTTP ${response.status}，accepted=${accepted ? "true" : "false"}`, 500);

    logWarn(env, "cron.dispatch.unaccepted_response", {
      deliveryId: message.deliveryId,
      instanceId: message.instanceId,
      siteId: message.siteId,
      attemptNo,
      httpStatus,
      accepted,
      dedupHit,
      durationMs: Date.now() - dispatchStartMs,
    });
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      timeout = true;
      errorCode = "REQUEST_TIMEOUT";
      errorMessage = `请求超时（>${requestTimeoutMs}ms）`;
      logWarn(env, "cron.dispatch.request_timeout", {
        deliveryId: message.deliveryId,
        instanceId: message.instanceId,
        siteId: message.siteId,
        attemptNo,
        requestTimeoutMs,
        durationMs: Date.now() - dispatchStartMs,
      });
    } else {
      errorCode = "REQUEST_FAILED";
      errorMessage = truncate(errorToMessage(error, "请求实例失败"), 500);
      logError(env, "cron.dispatch.request_failed", {
        deliveryId: message.deliveryId,
        instanceId: message.instanceId,
        siteId: message.siteId,
        attemptNo,
        errorMessage,
        durationMs: Date.now() - dispatchStartMs,
      });
    }

    await insertDeliveryAttempt({
      db: env.DB,
      deliveryId: message.deliveryId,
      attemptNo,
      startedAt,
      endedAt: nowIso(),
      httpStatus,
      timeout,
      errorCode,
      errorMessage,
    });
  }

  await markDeliveryFailed({
    db: env.DB,
    deliveryId: message.deliveryId,
    attemptCount: attemptNo,
    responseStatus: httpStatus,
    accepted,
    dedupHit,
    errorCode: errorCode ?? "UNKNOWN_ERROR",
    errorMessage: errorMessage ?? "未知错误",
  });

  logWarn(env, "cron.dispatch.failed_marked_retry", {
    deliveryId: message.deliveryId,
    instanceId: message.instanceId,
    siteId: message.siteId,
    attemptNo,
    httpStatus,
    errorCode: errorCode ?? "UNKNOWN_ERROR",
    errorMessage: errorMessage ?? "未知错误",
    durationMs: Date.now() - dispatchStartMs,
  });

  return "retry";
}
