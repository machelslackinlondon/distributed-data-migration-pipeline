import { describe, expect, test } from "@jest/globals";

import {
  loadProfiles,
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
      },
      note: "Observed local result only; do not extrapolate production capacity.",
    });
  });
});
