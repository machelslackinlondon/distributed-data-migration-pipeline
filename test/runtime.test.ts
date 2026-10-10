import { afterEach, describe, expect, test } from "@jest/globals";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { PipelineConfig } from "../src/config.js";
import { createRuntime, type MigrationRuntime } from "../src/runtime.js";
import { openDatabase } from "../src/storage/database.js";
import { SourceRepository } from "../src/storage/repositories.js";

describe("createRuntime", () => {
  let directory: string | undefined;
  let runtime: MigrationRuntime | undefined;

  afterEach(async () => {
    await runtime?.close({ drain: false });
    if (directory !== undefined) {
      rmSync(directory, { recursive: true, force: true });
    }
  });

  test("replays unfinished durable events at startup and closes cleanly", async () => {
    directory = mkdtempSync(join(tmpdir(), "migration-runtime-"));
    const databasePath = join(directory, "migration.sqlite");
    const database = openDatabase(databasePath);
    new SourceRepository(database).append({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: "Ada", email: "ada@example.com", status: "active" },
    });
    database.close();

    runtime = await createRuntime(config(databasePath), {
      logger: false,
      summaries: false,
    });
    await runtime.pipeline.awaitIdle(2_000);

    expect(runtime.target.getRecord("record-1")).toBeDefined();
    expect(runtime.checkpoints.get()).toBe(1);

    await runtime.close({ drain: true });
    expect(runtime.pipeline.status().state).toBe("stopped");
    runtime = undefined;
  });
});

function config(databasePath: string): PipelineConfig {
  return {
    databasePath,
    queueCapacity: 10,
    batchSize: 2,
    maxBatchSize: 5,
    maxBatchWaitMs: 5,
    workers: 2,
    targetConcurrency: 1,
    targetTimeoutMs: 100,
    maxAttempts: 2,
    retryBaseMs: 1,
    retryMaxMs: 10,
    retryCapacity: 10,
    payloadLimitBytes: 65_536,
  };
}
