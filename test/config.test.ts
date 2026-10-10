import { describe, expect, test } from "@jest/globals";

import { loadConfig } from "../src/config.js";

describe("loadConfig", () => {
  test("returns conservative bounded defaults", () => {
    const config = loadConfig({});

    expect(config).toEqual({
      databasePath: "./data/migration.sqlite",
      queueCapacity: 1_000,
      batchSize: 50,
      maxBatchSize: 500,
      maxBatchWaitMs: 100,
      workers: 4,
      targetConcurrency: 2,
      targetTimeoutMs: 2_000,
      maxAttempts: 5,
      retryBaseMs: 100,
      retryMaxMs: 10_000,
      retryCapacity: 2_000,
      payloadLimitBytes: 65_536,
    });
  });

  test.each([
    ["QUEUE_CAPACITY", "0", "QUEUE_CAPACITY must be a positive integer"],
    ["WORKERS", "1.5", "WORKERS must be a positive integer"],
    ["MAX_ATTEMPTS", "many", "MAX_ATTEMPTS must be a positive integer"],
  ])("rejects invalid %s", (name, value, message) => {
    expect(() => loadConfig({ [name]: value })).toThrow(message);
  });

  test("rejects a batch size above the hard maximum", () => {
    expect(() =>
      loadConfig({ BATCH_SIZE: "101", MAX_BATCH_SIZE: "100" }),
    ).toThrow("BATCH_SIZE must not exceed MAX_BATCH_SIZE");
  });

  test("rejects target concurrency above worker count", () => {
    expect(() => loadConfig({ WORKERS: "2", TARGET_CONCURRENCY: "3" })).toThrow(
      "TARGET_CONCURRENCY must not exceed WORKERS",
    );
  });
});
