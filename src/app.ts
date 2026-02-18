import { Hono } from "hono";

import { SIGNATURE_WINDOW_MS, TELEMETRY_SCHEMA_VERSION } from "./constants";
import {
  disableInstance,
  getHealthSummary,
  getInstanceBySiteId,
  getInstanceStatusBySiteId,
  insertBuildEvent,
  upsertInstance,
} from "./db-instance";
import {
  deregisterRequestSchema,
  statusRequestSchema,
  syncRequestSchema,
} from "./schemas";
import {
  ensureSignatureFresh,
  getActiveKid,
  parsePublicJwks,
  verifySignedPayload,
} from "./security";
import type { EnvBindings } from "./types";
import {
  errorToMessage,
  evaluateSiteUrl,
  normalizeIsoOrFallback,
  normalizeNullableString,
  nowIso,
  safeReadJson,
} from "./utils";

export const app = new Hono<{ Bindings: EnvBindings }>();

app.get("/", (c) => {
  return c.json({
    ok: true,
    data: {
      service: "neutralpress-cloud",
      status: "online",
      schemaVer: TELEMETRY_SCHEMA_VERSION,
      now: nowIso(),
    },
  });
});

app.get("/v1/health", async (c) => {
  const summary = await getHealthSummary(c.env.DB);

  return c.json({
    ok: true,
    data: {
      now: nowIso(),
      instances: summary.instances,
      deliveriesLast24h: summary.deliveries,
    },
  });
});

app.get("/.well-known/jwks.json", (c) => {
  try {
    const jwks = parsePublicJwks(c.env);
    return c.json(jwks, 200, {
      "cache-control": "public, max-age=300, stale-while-revalidate=60",
    });
  } catch (error) {
    return c.json(
      {
        ok: false,
        error: {
          code: "JWKS_PARSE_ERROR",
          message: errorToMessage(error, "无法解析 JWKS"),
        },
      },
      500,
    );
  }
});

app.post("/v1/instances/sync", async (c) => {
  const parsed = syncRequestSchema.safeParse(await safeReadJson(c.req.raw));
  if (!parsed.success) {
    return c.json(
      {
        ok: false,
        error: {
          code: "BAD_REQUEST",
          message: parsed.error.issues[0]?.message ?? "请求体格式错误",
        },
      },
      400,
    );
  }

  const input = parsed.data;
  if (!ensureSignatureFresh(input.signature, SIGNATURE_WINDOW_MS)) {
    return c.json(
      {
        ok: false,
        error: {
          code: "SIGNATURE_TIMESTAMP_EXPIRED",
          message: "签名时间戳已过期或不合法",
        },
      },
      401,
    );
  }

  const existing = await getInstanceBySiteId(c.env.DB, input.siteId);
  const verifyPublicKey = existing ? existing.site_pub_key : input.sitePubKey;

  const payloadToVerify: Record<string, unknown> = {
    siteId: input.siteId,
    sitePubKey: input.sitePubKey,
    siteKeyAlg: input.siteKeyAlg,
    siteUrl: input.siteUrl ?? null,
    appVersion: input.appVersion ?? null,
    buildId: input.buildId ?? null,
    commit: input.commit ?? null,
    builtAt: input.builtAt ?? null,
    idempotencyKey: input.idempotencyKey ?? null,
  };

  if (input.minuteOfDay !== undefined) {
    payloadToVerify.minuteOfDay = input.minuteOfDay;
  }

  const verified = await verifySignedPayload({
    method: "POST",
    path: "/v1/instances/sync",
    payload: payloadToVerify,
    signature: input.signature,
    publicKey: verifyPublicKey,
  });

  if (!verified) {
    return c.json(
      {
        ok: false,
        error: {
          code: "INVALID_SIGNATURE",
          message: "实例签名校验失败",
        },
      },
      401,
    );
  }

  const normalizedUrl = evaluateSiteUrl(input.siteUrl ?? null);
  const appVersion = normalizeNullableString(input.appVersion);
  const buildId = normalizeNullableString(input.buildId);
  const commitHash = normalizeNullableString(input.commit);
  const builtAt = normalizeIsoOrFallback(input.builtAt, nowIso());

  const upsertResult = await upsertInstance({
    db: c.env.DB,
    existing,
    siteId: input.siteId,
    sitePubKey: input.sitePubKey,
    siteKeyAlg: input.siteKeyAlg,
    normalizedSiteUrl: normalizedUrl.url,
    pendingReason: normalizedUrl.pendingReason,
    preferredMinuteOfDay: input.minuteOfDay ?? null,
    appVersion,
    buildId,
    commitHash,
  });

  const idempotencyKey =
    normalizeNullableString(input.idempotencyKey) ??
    `${input.siteId}:${buildId ?? "no-build-id"}:${builtAt}`;

  await insertBuildEvent({
    db: c.env.DB,
    instanceId: upsertResult.instanceId,
    builtAt,
    appVersion,
    buildId,
    commitHash,
    idempotencyKey,
  });

  return c.json({
    ok: true,
    data: {
      siteId: input.siteId,
      instanceId: upsertResult.instanceId,
      status: upsertResult.status,
      pendingReason: normalizedUrl.pendingReason,
      minuteOfDay: upsertResult.minuteOfDay,
      nextRunAt: upsertResult.nextRunAt,
      cloudActiveKid: getActiveKid(c.env),
      syncedAt: nowIso(),
    },
  });
});

app.post("/v1/instances/deregister", async (c) => {
  const parsed = deregisterRequestSchema.safeParse(await safeReadJson(c.req.raw));
  if (!parsed.success) {
    return c.json(
      {
        ok: false,
        error: {
          code: "BAD_REQUEST",
          message: parsed.error.issues[0]?.message ?? "请求体格式错误",
        },
      },
      400,
    );
  }

  const input = parsed.data;
  if (!ensureSignatureFresh(input.signature, SIGNATURE_WINDOW_MS)) {
    return c.json(
      {
        ok: false,
        error: {
          code: "SIGNATURE_TIMESTAMP_EXPIRED",
          message: "签名时间戳已过期或不合法",
        },
      },
      401,
    );
  }

  const existing = await getInstanceBySiteId(c.env.DB, input.siteId);
  if (!existing) {
    return c.json(
      {
        ok: false,
        error: {
          code: "INSTANCE_NOT_FOUND",
          message: "实例不存在",
        },
      },
      404,
    );
  }

  const payloadToVerify = {
    siteId: input.siteId,
    reason: input.reason ?? null,
    requestedAt: input.requestedAt ?? null,
  };

  const verified = await verifySignedPayload({
    method: "POST",
    path: "/v1/instances/deregister",
    payload: payloadToVerify,
    signature: input.signature,
    publicKey: existing.site_pub_key,
  });

  if (!verified) {
    return c.json(
      {
        ok: false,
        error: {
          code: "INVALID_SIGNATURE",
          message: "实例签名校验失败",
        },
      },
      401,
    );
  }

  await disableInstance({
    db: c.env.DB,
    siteId: input.siteId,
    reason: normalizeNullableString(input.reason),
  });

  return c.json({
    ok: true,
    data: {
      siteId: input.siteId,
      instanceId: existing.instance_id,
      status: "disabled",
      deregisteredAt: nowIso(),
    },
  });
});

app.post("/v1/instances/status", async (c) => {
  const parsed = statusRequestSchema.safeParse(await safeReadJson(c.req.raw));
  if (!parsed.success) {
    return c.json(
      {
        ok: false,
        error: {
          code: "BAD_REQUEST",
          message: parsed.error.issues[0]?.message ?? "请求体格式错误",
        },
      },
      400,
    );
  }

  const input = parsed.data;
  if (!ensureSignatureFresh(input.signature, SIGNATURE_WINDOW_MS)) {
    return c.json(
      {
        ok: false,
        error: {
          code: "SIGNATURE_TIMESTAMP_EXPIRED",
          message: "签名时间戳已过期或不合法",
        },
      },
      401,
    );
  }

  const existing = await getInstanceBySiteId(c.env.DB, input.siteId);
  if (!existing) {
    return c.json(
      {
        ok: false,
        error: {
          code: "INSTANCE_NOT_FOUND",
          message: "实例不存在",
        },
      },
      404,
    );
  }

  const payloadToVerify = {
    siteId: input.siteId,
    requestedAt: input.requestedAt ?? null,
  };

  const verified = await verifySignedPayload({
    method: "POST",
    path: "/v1/instances/status",
    payload: payloadToVerify,
    signature: input.signature,
    publicKey: existing.site_pub_key,
  });

  if (!verified) {
    return c.json(
      {
        ok: false,
        error: {
          code: "INVALID_SIGNATURE",
          message: "实例签名校验失败",
        },
      },
      401,
    );
  }

  const status = await getInstanceStatusBySiteId(c.env.DB, input.siteId);
  if (!status) {
    return c.json(
      {
        ok: false,
        error: {
          code: "INSTANCE_NOT_FOUND",
          message: "实例不存在",
        },
      },
      404,
    );
  }

  return c.json({
    ok: true,
    data: {
      ...status,
      now: nowIso(),
    },
  });
});

app.notFound((c) => {
  return c.json(
    {
      ok: false,
      error: {
        code: "NOT_FOUND",
        message: "路由不存在",
      },
    },
    404,
  );
});

app.onError((error, c) => {
  console.error("Unhandled error:", error);
  return c.json(
    {
      ok: false,
      error: {
        code: "INTERNAL_ERROR",
        message: "服务内部错误",
      },
    },
    500,
  );
});
