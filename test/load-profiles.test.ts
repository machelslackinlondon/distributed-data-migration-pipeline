import { describe, expect, test } from "@jest/globals";

import {
  loadProfiles,
  parseLoadMetrics,
  parseProfileName,
  shapeLoadResult,
} from "../scripts/load-profiles.js";

describe("load test profiles", () => {
  test("defines every required bounded workload", () => {
    expect(Object.keys(loadProfiles).sort()).toEqual([
      "burst",
      "large-batches",
      "outage",
      "producer-heavy",
      "recovery",
      "reduced-workers",
      "slow-target",
      "small-batches",
      "steady",
    ]);

    for (const profile of Object.values(loadProfiles)) {
      expect(profile.durationSeconds).toBeGreaterThan(0);
      expect(profile.durationSeconds).toBeLessThanOrEqual(30);
      expect(profile.connections).toBeGreaterThan(0);
      expect(profile.overallRate).toBeGreaterThan(0);
    }
  });

  test("parses a named profile and rejects unknown names", () => {
    expect(parseProfileName(["--profile=slow-target"])).toBe("slow-target");
    expect(() => parseProfileName(["--profile=unbounded"])).toThrow(
      "unknown load profile",
    );
  });

  test("shapes observed measurements without extrapolating capacity", () => {
    const output = shapeLoadResult({
      profile: "steady",
      requestsPerSecond: 900,
      latencyP99Ms: 12,
      errors: 2,
      timeouts: 1,
      non2xx: 3,
      durationSeconds: 5,
      startStatus: {
        state: "running",
        queueDepth: 0,
        activeWorkers: 0,
        scheduledRetries: 0,
        checkpointPosition: 0,
        sourcePosition: 0,
        checkpointLag: 0,
      },
      endStatus: {
        state: "running",
        queueDepth: 4,
        activeWorkers: 2,
        scheduledRetries: 1,
        checkpointPosition: 4_000,
        sourcePosition: 4_100,
        checkpointLag: 100,
      },
      cpuUserMicros: 100_000,
      cpuSystemMicros: 20_000,
      residentMemoryBytes: 80_000_000,
      targetCount: 3_900,
      deadLetterCount: 5,
      processingLatencyTotalSeconds: 20,
      processedCount: 4_000,
      batchLatencyTotalSeconds: 10,
      batchCount: 200,
      retryCount: 25,
      deadLetteredCount: 5,
    });

    expect(output).toMatchObject({
      profile: "steady",
      observed: {
        requestsPerSecond: 900,
        targetThroughputRecordsPerSecond: 800,
        targetUniqueRecordCount: 3_900,
        checkpointRecordsPerSecond: 800,
        queueDepth: 4,
        retryDepth: 1,
        deadLetterCount: 5,
        processingLatencyAverageMs: 5,
        batchLatencyAverageMs: 50,
        retryRatePerSecond: 5,
        deadLetterRatePerSecond: 1,
      },
      note: "Observed local result only; do not extrapolate production capacity.",
    });
  });

  test("extracts load measurements from Prometheus metrics", () => {
    const metrics = `
migration_processing_latency_seconds_sum 0.25
migration_processing_latency_seconds_count 10
migration_batch_latency_seconds_sum 0.1
migration_batch_latency_seconds_count 4
migration_records_retried_total 8
migration_records_dead_lettered_total 2
`;

    expect(parseLoadMetrics(metrics)).toEqual({
      processingLatencyTotalSeconds: 0.25,
      processedCount: 10,
      batchLatencyTotalSeconds: 0.1,
      batchCount: 4,
      retryCount: 8,
      deadLetteredCount: 2,
    });
  });
});
