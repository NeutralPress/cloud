import type { EnvBindings } from "./types";
import { nowIso } from "./utils";

type LogLevel = "debug" | "info" | "warn" | "error";

const levelRank: Record<LogLevel, number> = {
  debug: 10,
  info: 20,
  warn: 30,
  error: 40,
};

function normalizeLogLevel(raw: string | undefined): LogLevel {
  if (!raw) return "info";
  const normalized = raw.trim().toLowerCase();
  if (normalized === "debug") return "debug";
  if (normalized === "info") return "info";
  if (normalized === "warn" || normalized === "warning") return "warn";
  if (normalized === "error") return "error";
  return "info";
}

function shouldLog(env: EnvBindings, level: LogLevel): boolean {
  const minLevel = normalizeLogLevel(env.LOG_LEVEL);
  return levelRank[level] >= levelRank[minLevel];
}

function safeJsonStringify(value: unknown): string {
  try {
    return JSON.stringify(value, (_key, item) => {
      if (item instanceof Error) {
        return {
          name: item.name,
          message: item.message,
          stack: item.stack,
        };
      }
      if (typeof item === "bigint") {
        return item.toString();
      }
      return item;
    });
  } catch {
    return JSON.stringify({
      ts: nowIso(),
      level: "error",
      event: "logger.stringify_failed",
      message: "日志对象无法序列化",
    });
  }
}

function emit(level: LogLevel, event: string, payload?: Record<string, unknown>): void {
  const line = safeJsonStringify({
    ts: nowIso(),
    level,
    event,
    ...(payload ?? {}),
  });

  if (level === "error") {
    console.error(line);
    return;
  }
  if (level === "warn") {
    console.warn(line);
    return;
  }
  console.log(line);
}

export function logDebug(
  env: EnvBindings,
  event: string,
  payload?: Record<string, unknown>,
): void {
  if (!shouldLog(env, "debug")) return;
  emit("debug", event, payload);
}

export function logInfo(
  env: EnvBindings,
  event: string,
  payload?: Record<string, unknown>,
): void {
  if (!shouldLog(env, "info")) return;
  emit("info", event, payload);
}

export function logWarn(
  env: EnvBindings,
  event: string,
  payload?: Record<string, unknown>,
): void {
  if (!shouldLog(env, "warn")) return;
  emit("warn", event, payload);
}

export function logError(
  env: EnvBindings,
  event: string,
  payload?: Record<string, unknown>,
): void {
  if (!shouldLog(env, "error")) return;
  emit("error", event, payload);
}
