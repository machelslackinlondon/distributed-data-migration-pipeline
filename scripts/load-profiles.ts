import type { PipelineConfig } from "../src/config.js";
import type { PipelineStatus } from "../src/domain.js";

export type FaultMode =
  | { kind: "none" }
  | { kind: "delay"; delayMs: number }
  | { kind: "outage" }
  | { kind: "recover"; failures: number };

export interface LoadProfile {
  connections: number;
  durationSeconds: number;
  overallRate: number;
  config: Partial<PipelineConfig>;
  fault: FaultMode;
}

export const loadProfiles = {
  steady: profile(10, 10, 500),
  burst: profile(50, 10, 5_000, { queueCapacity: 2_000 }),
  "producer-heavy": profile(
    100,
    10,
    10_000,
    { queueCapacity: 500, workers: 2, targetConcurrency: 2 },
    { kind: "delay", delayMs: 2 },
  ),
  "slow-target": profile(20, 10, 2_000, {}, { kind: "delay", delayMs: 25 }),
  outage: profile(20, 10, 1_000, {}, { kind: "outage" }),
  recovery: profile(20, 10, 1_000, {}, { kind: "recover", failures: 500 }),
  "reduced-workers": profile(20, 10, 2_000, {
    workers: 1,
    targetConcurrency: 1,
  }),
  "large-batches": profile(20, 10, 2_000, {
    batchSize: 500,
    maxBatchSize: 500,
    maxBatchWaitMs: 250,
  }),
  "small-batches": profile(20, 10, 2_000, {
    batchSize: 1,
    maxBatchSize: 1,
    maxBatchWaitMs: 1,
  }),
} as const satisfies Record<string, LoadProfile>;

export type LoadProfileName = keyof typeof loadProfiles;

export function parseProfileName(arguments_: string[]): LoadProfileName {
  const raw = arguments_.find((argument) => argument.startsWith("--profile="));
  const name = raw?.slice("--profile=".length) ?? "steady";
  if (!(name in loadProfiles)) {
    throw new Error(
      `unknown load profile '${name}'; expected one of ${Object.keys(loadProfiles).join(", ")}`,
    );
  }
  return name as LoadProfileName;
}

export interface RawLoadResult {
  profile: LoadProfileName;
  requestsPerSecond: number;
  latencyP99Ms: number;
  errors: number;
  timeouts: number;
  non2xx: number;
  durationSeconds: number;
  startStatus: PipelineStatus;
  endStatus: PipelineStatus;
  cpuUserMicros: number;
  cpuSystemMicros: number;
  residentMemoryBytes: number;
  targetCount: number;
  deadLetterCount: number;
  processingLatencyTotalSeconds: number;
  processedCount: number;
  batchLatencyTotalSeconds: number;
  batchCount: number;
  retryCount: number;
  deadLetteredCount: number;
}

export function shapeLoadResult(input: RawLoadResult) {
  return {
    profile: input.profile,
    testedConfiguration: loadProfiles[input.profile],
    observed: {
      requestsPerSecond: input.requestsPerSecond,
      latencyP99Ms: input.latencyP99Ms,
      errors: input.errors,
      timeouts: input.timeouts,
      non2xx: input.non2xx,
      queueDepth: input.endStatus.queueDepth,
      activeWorkers: input.endStatus.activeWorkers,
      retryDepth: input.endStatus.scheduledRetries,
      checkpointPosition: input.endStatus.checkpointPosition,
      checkpointLag: input.endStatus.checkpointLag,
      checkpointRecordsPerSecond: rate(
        input.endStatus.checkpointPosition -
          input.startStatus.checkpointPosition,
        input.durationSeconds,
      ),
      targetThroughputRecordsPerSecond: rate(
        input.endStatus.checkpointPosition -
          input.startStatus.checkpointPosition,
        input.durationSeconds,
      ),
      targetUniqueRecordCount: input.targetCount,
      deadLetterCount: input.deadLetterCount,
      processingLatencyAverageMs: averageMilliseconds(
        input.processingLatencyTotalSeconds,
        input.processedCount,
      ),
      batchLatencyAverageMs: averageMilliseconds(
        input.batchLatencyTotalSeconds,
        input.batchCount,
      ),
      retryRatePerSecond: rate(input.retryCount, input.durationSeconds),
      deadLetterRatePerSecond: rate(
        input.deadLetteredCount,
        input.durationSeconds,
      ),
      cpuUserMicros: input.cpuUserMicros,
      cpuSystemMicros: input.cpuSystemMicros,
      residentMemoryBytes: input.residentMemoryBytes,
    },
    note: "Observed local result only; do not extrapolate production capacity.",
  };
}

export function parseLoadMetrics(
  metrics: string,
): Pick<
  RawLoadResult,
  | "processingLatencyTotalSeconds"
  | "processedCount"
  | "batchLatencyTotalSeconds"
  | "batchCount"
  | "retryCount"
  | "deadLetteredCount"
> {
  return {
    processingLatencyTotalSeconds: metricValue(
      metrics,
      "migration_processing_latency_seconds_sum",
    ),
    processedCount: metricValue(
      metrics,
      "migration_processing_latency_seconds_count",
    ),
    batchLatencyTotalSeconds: metricValue(
      metrics,
      "migration_batch_latency_seconds_sum",
    ),
    batchCount: metricValue(metrics, "migration_batch_latency_seconds_count"),
    retryCount: metricValue(metrics, "migration_records_retried_total"),
    deadLetteredCount: metricValue(
      metrics,
      "migration_records_dead_lettered_total",
    ),
  };
}

function profile(
  connections: number,
  durationSeconds: number,
  overallRate: number,
  config: Partial<PipelineConfig> = {},
  fault: FaultMode = { kind: "none" },
): LoadProfile {
  return { connections, durationSeconds, overallRate, config, fault };
}

function rate(count: number, seconds: number): number {
  return Math.round((count / seconds) * 100) / 100;
}

function averageMilliseconds(totalSeconds: number, count: number): number {
  return count === 0 ? 0 : round((totalSeconds * 1_000) / count);
}

function metricValue(metrics: string, name: string): number {
  const prefix = `${name} `;
  const line = metrics
    .split("\n")
    .find((candidate) => candidate.startsWith(prefix));
  const value =
    line === undefined ? Number.NaN : Number(line.slice(prefix.length));
  if (!Number.isFinite(value)) {
    throw new Error(`load metric '${name}' is missing or invalid`);
  }
  return value;
}

function round(value: number): number {
  return Math.round(value * 100) / 100;
}
