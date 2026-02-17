import { TELEMETRY_SCHEMA_VERSION } from "./constants";
import type { ParsedTelemetry } from "./types";
import {
  asRecord,
  readBoolean,
  readNumber,
  readString,
  truncateByBytes,
} from "./utils";

export function parseTelemetry(
  payload: unknown,
  options: {
    fallbackCollectedAt: string;
    rawMaxBytes: number;
    rawText: string;
  },
): ParsedTelemetry {
  const root = asRecord(payload);
  const data = asRecord(root?.data) ?? {};

  const protocol = asRecord(data.protocolVerification);
  const config = asRecord(data.configSnapshot);
  const latest = asRecord(data.latestCronSummary);
  const tasks = asRecord(data.taskDurations);
  const health = asRecord(data.runtimeHealth);
  const version = asRecord(data.versionInfo);

  return {
    schemaVer:
      readString(data.schemaVer) ?? readString(root?.schemaVer) ?? TELEMETRY_SCHEMA_VERSION,
    collectedAt: readString(data.collectedAt) ?? options.fallbackCollectedAt,
    accepted:
      readBoolean(protocol?.accepted) ??
      readBoolean(data.accepted) ??
      readBoolean(root?.accepted) ??
      false,
    dedupHit:
      readBoolean(protocol?.dedupHit) ??
      readBoolean(data.dedupHit) ??
      readBoolean(root?.dedupHit) ??
      false,
    verifySource: readString(protocol?.verifySource),
    dnssecAd: readBoolean(protocol?.dnssecAd),
    verifyMs: readNumber(protocol?.verifyMs),
    tokenAgeMs: readNumber(protocol?.tokenAgeMs),
    cronEnabled: readBoolean(config?.cronEnabled),
    doctorEnabled: readBoolean(config?.doctorEnabled),
    projectsEnabled: readBoolean(config?.projectsEnabled),
    friendsEnabled: readBoolean(config?.friendsEnabled),
    latestRunId: readNumber(latest?.latestRunId),
    latestCreatedAt: readString(latest?.latestCreatedAt),
    latestStatus: readString(latest?.latestStatus),
    latestDurationMs: readNumber(latest?.latestDurationMs),
    enabledCount: readNumber(latest?.enabledCount),
    successCount: readNumber(latest?.successCount),
    failedCount: readNumber(latest?.failedCount),
    skippedCount: readNumber(latest?.skippedCount),
    doctorDurationMs: readNumber(tasks?.doctorDurationMs),
    projectsDurationMs: readNumber(tasks?.projectsDurationMs),
    friendsDurationMs: readNumber(tasks?.friendsDurationMs),
    healthRecordId: readNumber(health?.healthRecordId),
    healthCreatedAt: readString(health?.healthCreatedAt),
    healthStatus: readString(health?.healthStatus),
    healthOkCount: readNumber(health?.healthOkCount),
    healthWarningCount: readNumber(health?.healthWarningCount),
    healthErrorCount: readNumber(health?.healthErrorCount),
    dbLatencyMs: readNumber(health?.dbLatencyMs),
    redisLatencyMs: readNumber(health?.redisLatencyMs),
    siteSelfLatencyMs: readNumber(health?.siteSelfLatencyMs),
    appVersion: readString(version?.appVersion),
    runtimeNodeVersion: readString(version?.runtimeNodeVersion),
    buildId: readString(version?.buildId),
    commitHash: readString(version?.commit),
    rawJson: truncateByBytes(options.rawText, options.rawMaxBytes),
  };
}
