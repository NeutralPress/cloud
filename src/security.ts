import { SignJWT, importJWK, type JWK } from "jose";

import type { SignaturePayload } from "./schemas";
import type { EnvBindings } from "./types";
import {
  canonicalStringify,
  decodeBase64Flexible,
  decodePem,
  errorToMessage,
  extractDnsOrRawKey,
  isFreshTimestamp,
  normalizeNullableString,
  sha256Base64Url,
} from "./utils";

interface SignContext {
  activeKid: string;
  privateKeys: Map<string, JWK>;
}

const privateKeyCryptoCache = new Map<string, CryptoKey>();

export function ensureSignatureFresh(
  signature: SignaturePayload,
  toleranceMs: number,
): boolean {
  return isFreshTimestamp(signature.ts, toleranceMs);
}

export async function verifySignedPayload(input: {
  method: string;
  path: string;
  payload: Record<string, unknown>;
  signature: SignaturePayload;
  publicKey: string;
}): Promise<boolean> {
  const bodyHash = await sha256Base64Url(canonicalStringify(input.payload));
  const message = [
    "NP-CLOUD-SIGN-V1",
    input.method.toUpperCase(),
    input.path,
    bodyHash,
    input.signature.ts,
    input.signature.nonce,
  ].join("\n");

  const verifyKey = await importEd25519PublicKey(input.publicKey).catch(() => null);
  if (!verifyKey) return false;

  const signatureBytes = decodeBase64Flexible(input.signature.sig);
  const messageBytes = new TextEncoder().encode(message);

  return await crypto.subtle.verify("Ed25519", verifyKey, signatureBytes, messageBytes);
}

export async function signCloudTriggerToken(
  env: EnvBindings,
  input: {
    deliveryId: string;
    siteId: string;
  },
): Promise<string> {
  const context = parseSignContext(env);
  const privateJwk = context.privateKeys.get(context.activeKid);
  if (!privateJwk) {
    throw new Error(`找不到活跃私钥 kid=${context.activeKid}`);
  }

  const cacheKey = `kid:${context.activeKid}`;
  let privateKey = privateKeyCryptoCache.get(cacheKey);
  if (!privateKey) {
    privateKey = (await importJWK(privateJwk, "EdDSA")) as CryptoKey;
    privateKeyCryptoCache.set(cacheKey, privateKey);
  }

  if (!privateKey) {
    throw new Error("无法加载云签名私钥");
  }

  const nowSec = Math.floor(Date.now() / 1000);
  const issuer = env.CLOUD_ISSUER || "np-cloud";
  const audience = env.INSTANCE_TRIGGER_AUDIENCE || "np-instance";

  return await new SignJWT({
    deliveryId: input.deliveryId,
    siteId: input.siteId,
  })
    .setProtectedHeader({
      alg: "EdDSA",
      typ: "JWT",
      kid: context.activeKid,
    })
    .setIssuer(issuer)
    .setAudience(audience)
    .setSubject(input.siteId)
    .setJti(`jti_${crypto.randomUUID()}`)
    .setIssuedAt(nowSec)
    .setNotBefore(nowSec - 5)
    .setExpirationTime(nowSec + 60)
    .sign(privateKey);
}

export function parsePublicJwks(env: EnvBindings): { keys: JWK[] } {
  let parsed: unknown;
  try {
    parsed = JSON.parse(env.CLOUD_JWKS_JSON);
  } catch (error) {
    throw new Error(`CLOUD_JWKS_JSON 解析失败: ${errorToMessage(error, "invalid json")}`);
  }

  if (
    !parsed ||
    typeof parsed !== "object" ||
    !Array.isArray((parsed as { keys?: unknown }).keys)
  ) {
    throw new Error("CLOUD_JWKS_JSON 必须是 { keys: [...] } 结构");
  }

  return {
    keys: ((parsed as { keys: unknown[] }).keys.filter(Boolean) as JWK[]),
  };
}

export function getActiveKid(env: EnvBindings): string {
  return parseSignContext(env).activeKid;
}

function parseSignContext(env: EnvBindings): SignContext {
  const privateKeys = parsePrivateKeys(env.CLOUD_PRIVATE_KEYS_JSON);
  const activeKid =
    normalizeNullableString(env.CLOUD_ACTIVE_KID) ?? privateKeys.keys().next().value;

  if (!activeKid || !privateKeys.has(activeKid)) {
    throw new Error("CLOUD_ACTIVE_KID 未配置或在私钥集合中不存在");
  }

  return { activeKid, privateKeys };
}

function parsePrivateKeys(rawJson: string): Map<string, JWK> {
  let parsed: unknown;
  try {
    parsed = JSON.parse(rawJson);
  } catch (error) {
    throw new Error(`CLOUD_PRIVATE_KEYS_JSON 解析失败: ${errorToMessage(error, "invalid json")}`);
  }

  const map = new Map<string, JWK>();

  if (
    parsed &&
    typeof parsed === "object" &&
    Array.isArray((parsed as { keys?: unknown[] }).keys)
  ) {
    for (const item of (parsed as { keys: unknown[] }).keys) {
      if (!item || typeof item !== "object") continue;
      const jwk = item as JWK;
      if (typeof jwk.kid === "string" && jwk.kid.length > 0) {
        map.set(jwk.kid, jwk);
      }
    }
    return map;
  }

  if (!parsed || typeof parsed !== "object") {
    throw new Error("CLOUD_PRIVATE_KEYS_JSON 必须是对象或 JWK Set");
  }

  for (const [kid, value] of Object.entries(parsed as Record<string, unknown>)) {
    if (!value || typeof value !== "object") continue;
    const jwk = value as JWK;
    if (!jwk.kid) jwk.kid = kid;
    map.set(kid, jwk);
  }

  if (map.size === 0) {
    throw new Error("未解析出任何私钥");
  }

  return map;
}

async function importEd25519PublicKey(raw: string): Promise<CryptoKey> {
  const trimmed = raw.trim();
  if (trimmed.includes("BEGIN PUBLIC KEY")) {
    const der = decodePem(trimmed, "PUBLIC KEY");
    return await crypto.subtle.importKey("spki", der, "Ed25519", false, ["verify"]);
  }

  const material = extractDnsOrRawKey(trimmed);
  const decoded = decodeBase64Flexible(material);

  if (decoded.byteLength === 32) {
    return await crypto.subtle.importKey("raw", decoded, "Ed25519", false, ["verify"]);
  }

  return await crypto.subtle.importKey("spki", decoded, "Ed25519", false, ["verify"]);
}
