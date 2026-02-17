import {
  MAX_SCHEDULE_SCAN_PER_TICK,
  SCHEDULE_BATCH_LIMIT,
} from "./constants";
import {
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

  let totalEnqueued = 0;

  while (totalEnqueued < MAX_SCHEDULE_SCAN_PER_TICK) {
    const rows = await getDueInstances(env.DB, now, SCHEDULE_BATCH_LIMIT);
    if (rows.length === 0) break;

    for (const row of rows) {
      if (totalEnqueued >= MAX_SCHEDULE_SCAN_PER_TICK) break;

      const deliveryId = generateDeliveryId();
      const enqueuedAt = nowIso();
      const message: DispatchMessage = {
        deliveryId,
        instanceId: row.instance_id,
        siteId: row.site_id,
        siteUrl: row.site_url,
        scheduledFor: row.next_run_at,
        enqueuedAt,
      };

      await createDelivery({
        db: env.DB,
        deliveryId,
        instanceId: row.instance_id,
        scheduledFor: row.next_run_at,
        enqueuedAt,
      });

      try {
        await env.DISPATCH_QUEUE.send(message);
      } catch (error) {
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

        continue;
      }

      await updateInstanceNextRun(
        env.DB,
        row.instance_id,
        computeNextRunAt(row.minute_of_day, nowDate),
      );

      totalEnqueued += 1;
    }
  }

  if (nowDate.getUTCMinutes() === 13) {
    await runMaintenance(env.DB);
  }
}

export async function handleQueue(
  batch: MessageBatch<DispatchMessage>,
  env: EnvBindings,
): Promise<void> {
  if (batch.queue.endsWith("-dlq")) {
    await handleDlq(batch, env);
    return;
  }

  const maxAttempts = parsePositiveInt(env.MAX_RETRY_ATTEMPTS, 6);

  for (const message of batch.messages) {
    const parsed = dispatchMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      console.error("Invalid queue payload:", parsed.error.flatten());
      message.ack();
      continue;
    }

    const attemptNo = message.attempts || 1;
    const result = await dispatchToInstance(parsed.data, env, attemptNo);

    if (result === "success" || result === "drop") {
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
      message.ack();
      continue;
    }

    message.retry();
  }
}

async function handleDlq(
  batch: MessageBatch<DispatchMessage>,
  env: EnvBindings,
): Promise<void> {
  for (const message of batch.messages) {
    const parsed = dispatchMessageSchema.safeParse(message.body);
    if (!parsed.success) {
      message.ack();
      continue;
    }

    await markDeliveryDead({
      db: env.DB,
      deliveryId: parsed.data.deliveryId,
      errorCode: "DLQ_REACHED",
      errorMessage: "消息进入死信队列",
    });

    message.ack();
  }
}

async function dispatchToInstance(
  message: DispatchMessage,
  env: EnvBindings,
  attemptNo: number,
): Promise<QueueResult> {
  const startedAt = nowIso();
  const requestTimeoutMs = parsePositiveInt(env.REQUEST_TIMEOUT_MS, 15000);
  const triggerPath = env.INSTANCE_TRIGGER_PATH || "/api/internal/cron/cloud-trigger";

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
      return "success";
    }

    errorCode = "UNACCEPTED_RESPONSE";
    errorMessage = truncate(`HTTP ${response.status}，accepted=${accepted ? "true" : "false"}`, 500);
  } catch (error) {
    if (error instanceof Error && error.name === "AbortError") {
      timeout = true;
      errorCode = "REQUEST_TIMEOUT";
      errorMessage = `请求超时（>${requestTimeoutMs}ms）`;
    } else {
      errorCode = "REQUEST_FAILED";
      errorMessage = truncate(errorToMessage(error, "请求实例失败"), 500);
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

  return "retry";
}
