export function nowIso(): string {
  return new Date().toISOString();
}

export function parsePositiveInt(raw: string | undefined, fallback: number): number {
  if (!raw) return fallback;
  const value = Number.parseInt(raw, 10);
  return Number.isFinite(value) && value > 0 ? value : fallback;
}

export function normalizeNullableString(value: unknown): string | null {
  if (typeof value !== "string") return null;
  const trimmed = value.trim();
  return trimmed.length > 0 ? trimmed : null;
}

export function normalizeIsoOrFallback(value: string | null | undefined, fallback: string): string {
  const normalized = normalizeNullableString(value);
  if (!normalized) return fallback;
  const parsed = Date.parse(normalized);
  if (!Number.isFinite(parsed)) return fallback;
  return new Date(parsed).toISOString();
}

export function truncate(value: string | null | undefined, maxLength: number): string | null {
  if (!value) return null;
  if (value.length <= maxLength) return value;
  return `${value.slice(0, maxLength - 3)}...`;
}

export function truncateByBytes(value: string, maxBytes: number): string {
  const encoder = new TextEncoder();
  const encoded = encoder.encode(value);
  if (encoded.byteLength <= maxBytes) return value;

  let end = maxBytes;
  while (end > 0 && (encoded[end] & 0b1100_0000) === 0b1000_0000) {
    end -= 1;
  }

  return new TextDecoder().decode(encoded.slice(0, end));
}

export function errorToMessage(error: unknown, fallback: string): string {
  if (error instanceof Error && error.message.trim().length > 0) {
    return error.message;
  }
  return fallback;
}

export async function safeReadJson(request: Request): Promise<unknown> {
  try {
    return await request.json();
  } catch {
    return {};
  }
}

export function safeJsonParse(value: string): unknown {
  try {
    return JSON.parse(value);
  } catch {
    return {};
  }
}

export function asRecord(value: unknown): Record<string, unknown> | null {
  if (!value || typeof value !== "object" || Array.isArray(value)) {
    return null;
  }
  return value as Record<string, unknown>;
}

export function readString(value: unknown): string | null {
  return typeof value === "string" && value.trim().length > 0
    ? value.trim()
    : null;
}

export function readBoolean(value: unknown): boolean | null {
  if (typeof value === "boolean") return value;
  if (typeof value === "number") {
    if (value === 0) return false;
    if (value === 1) return true;
  }
  if (typeof value === "string") {
    const normalized = value.trim().toLowerCase();
    if (normalized === "true" || normalized === "1") return true;
    if (normalized === "false" || normalized === "0") return false;
  }
  return null;
}

export function readNumber(value: unknown): number | null {
  if (typeof value === "number" && Number.isFinite(value)) return Math.round(value);
  if (typeof value === "string") {
    const parsed = Number.parseInt(value, 10);
    if (Number.isFinite(parsed)) return parsed;
  }
  return null;
}

export function toSqlBool(value: boolean): number {
  return value ? 1 : 0;
}

export function toSqlBoolOrNull(value: boolean | null): number | null {
  if (value === null) return null;
  return value ? 1 : 0;
}

export function isFreshTimestamp(timestamp: string, toleranceMs: number): boolean {
  const time = Date.parse(timestamp);
  if (!Number.isFinite(time)) return false;
  return Math.abs(Date.now() - time) <= toleranceMs;
}

export function canonicalStringify(value: unknown): string {
  return JSON.stringify(sortValue(value));
}

function sortValue(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map((item) => sortValue(item));
  }
  if (!value || typeof value !== "object") {
    return value;
  }

  const record = value as Record<string, unknown>;
  const sortedKeys = Object.keys(record).sort((a, b) => a.localeCompare(b));
  const result: Record<string, unknown> = {};

  for (const key of sortedKeys) {
    result[key] = sortValue(record[key]);
  }

  return result;
}

export async function sha256Base64Url(value: string): Promise<string> {
  const hashBuffer = await crypto.subtle.digest("SHA-256", new TextEncoder().encode(value));
  return encodeBase64Url(new Uint8Array(hashBuffer));
}

export function encodeBase64Url(bytes: Uint8Array): string {
  let binary = "";
  for (const byte of bytes) {
    binary += String.fromCharCode(byte);
  }
  return btoa(binary).replaceAll("+", "-").replaceAll("/", "_").replace(/=+$/g, "");
}

export function decodeBase64Flexible(value: string): Uint8Array<ArrayBuffer> {
  const trimmed = value.trim();
  const base64 = trimmed.replaceAll("-", "+").replaceAll("_", "/");
  const padded = base64 + "=".repeat((4 - (base64.length % 4)) % 4);
  const binary = atob(padded);

  const buffer = new ArrayBuffer(binary.length);
  const bytes = new Uint8Array(buffer);
  for (let i = 0; i < binary.length; i += 1) {
    bytes[i] = binary.charCodeAt(i);
  }
  return bytes;
}

export function decodePem(pem: string, label: string): ArrayBuffer {
  const header = `-----BEGIN ${label}-----`;
  const footer = `-----END ${label}-----`;
  const normalized = pem.replace(header, "").replace(footer, "").replace(/\s+/g, "");
  const decoded = decodeBase64Flexible(normalized);
  return decoded.buffer;
}

export function joinUrl(base: string, path: string): string {
  const normalizedBase = base.endsWith("/") ? base.slice(0, -1) : base;
  const normalizedPath = path.startsWith("/") ? path : `/${path}`;
  return `${normalizedBase}${normalizedPath}`;
}

export async function fetchWithTimeout(
  input: RequestInfo | URL,
  init: RequestInit,
  timeoutMs: number,
): Promise<Response> {
  const controller = new AbortController();
  const timer = setTimeout(() => controller.abort(), timeoutMs);

  try {
    return await fetch(input, {
      ...init,
      signal: controller.signal,
    });
  } finally {
    clearTimeout(timer);
  }
}

export function generateInstanceId(): string {
  return `ins_${crypto.randomUUID().replaceAll("-", "")}`;
}

export function generateDeliveryId(): string {
  return `dlv_${Date.now()}_${crypto.randomUUID().replaceAll("-", "").slice(0, 8)}`;
}

export function randomMinuteOfDay(): number {
  const random = new Uint16Array(1);
  crypto.getRandomValues(random);
  return random[0]! % 1440;
}

export function computeNextRunAt(minuteOfDay: number, baseDate: Date): string {
  const hour = Math.floor(minuteOfDay / 60);
  const minute = minuteOfDay % 60;

  const candidate = new Date(
    Date.UTC(
      baseDate.getUTCFullYear(),
      baseDate.getUTCMonth(),
      baseDate.getUTCDate(),
      hour,
      minute,
      0,
      0,
    ),
  );

  if (candidate.getTime() <= baseDate.getTime()) {
    candidate.setUTCDate(candidate.getUTCDate() + 1);
  }

  return candidate.toISOString();
}

export function evaluateSiteUrl(siteUrl: string | null): {
  url: string | null;
  pendingReason: string | null;
} {
  const raw = normalizeNullableString(siteUrl);
  if (!raw) {
    return { url: null, pendingReason: "pending_url_missing" };
  }

  let parsed: URL;
  try {
    parsed = new URL(raw);
  } catch {
    return { url: null, pendingReason: "pending_url_invalid" };
  }

  if (parsed.protocol !== "http:" && parsed.protocol !== "https:") {
    return { url: null, pendingReason: "pending_url_invalid_protocol" };
  }

  const hostname = parsed.hostname.toLowerCase();
  if (hostname === "example.com") {
    return { url: null, pendingReason: "pending_url_default_example" };
  }

  if (
    hostname === "localhost" ||
    hostname === "127.0.0.1" ||
    hostname === "::1" ||
    hostname.endsWith(".localhost") ||
    hostname.endsWith(".local") ||
    hostname.startsWith("127.")
  ) {
    return { url: null, pendingReason: "pending_url_localhost" };
  }

  return { url: `${parsed.protocol}//${parsed.host}`, pendingReason: null };
}

export function extractDnsOrRawKey(input: string): string {
  if (!input.includes(";") || !input.includes("=")) {
    return input;
  }

  const kv = new Map<string, string>();
  const parts = input.split(";");
  for (const part of parts) {
    const [key, ...rest] = part.split("=");
    if (!key || rest.length === 0) continue;
    kv.set(key.trim().toLowerCase(), rest.join("=").trim());
  }

  const p = kv.get("p");
  return p || input;
}

