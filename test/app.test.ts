import { afterEach, beforeEach, describe, expect, test } from "@jest/globals";

import { buildApp } from "../src/app.js";
import type { PipelineConfig } from "../src/config.js";
import { createMetrics } from "../src/observability.js";
import { CheckpointTracker } from "../src/pipeline/checkpoint.js";
import { MigrationPipeline } from "../src/pipeline/migration-pipeline.js";
import { TargetAdapter } from "../src/pipeline/target-adapter.js";
import { reconcile } from "../src/reconciliation.js";
import { openDatabase } from "../src/storage/database.js";
import {
  CheckpointRepository,
  DlqRepository,
  SourceRepository,
  TargetRepository,
} from "../src/storage/repositories.js";

describe("Fastify API", () => {
  let database: ReturnType<typeof openDatabase>;
  let pipeline: MigrationPipeline;
  let app: ReturnType<typeof buildApp>;
  let source: SourceRepository;
  let target: TargetRepository;
  let dlq: DlqRepository;

  beforeEach(async () => {
    const config = testConfig();
    database = openDatabase(":memory:");
    source = new SourceRepository(database);
    target = new TargetRepository(database);
    dlq = new DlqRepository(database);
    const metrics = createMetrics();
    pipeline = new MigrationPipeline({
      config,
      source,
      target: new TargetAdapter(target, {
        concurrency: config.targetConcurrency,
        timeoutMs: config.targetTimeoutMs,
      }),
      checkpoint: new CheckpointTracker(new CheckpointRepository(database)),
      dlq,
      observer: metrics,
    });
    await pipeline.start();
    app = buildApp({
      config,
      pipeline,
      metrics,
      runReconciliation: () => reconcile(source, target, dlq, 10),
    });
  });

  afterEach(async () => {
    await app.close();
    await pipeline.stop({ drain: false });
    database.close();
  });

  test("accepts an event and exposes health, metrics, and reconciliation", async () => {
    const event = eventPayload();
    const accepted = await app.inject({
      method: "POST",
      url: "/events",
      payload: event,
    });

    expect(accepted.statusCode).toBe(202);
    expect(accepted.json()).toMatchObject({ accepted: true, position: 1 });
    await pipeline.awaitIdle(2_000);

    const health = await app.inject({ method: "GET", url: "/health" });
    expect(health.statusCode).toBe(200);
    expect(health.json()).toMatchObject({
      status: "ok",
      ready: true,
      checkpointPosition: 1,
      checkpointLag: 0,
    });

    const metrics = await app.inject({ method: "GET", url: "/metrics" });
    expect(metrics.statusCode).toBe(200);
    expect(metrics.headers["content-type"]).toContain("text/plain");
    expect(metrics.body).toContain("migration_records_received_total 1");
    expect(metrics.body).toContain(
      'migration_records_processed_total{outcome="applied"} 1',
    );
    expect(metrics.body).toContain("migration_checkpoint_position 1");

    const reconciliation = await app.inject({
      method: "POST",
      url: "/reconciliation",
    });
    expect(reconciliation.statusCode).toBe(200);
    expect(reconciliation.json()).toMatchObject({ verified: true });
  });

  test("maps duplicate, malformed, and oversized ingress to explicit responses", async () => {
    const event = eventPayload();
    expect(
      (await app.inject({ method: "POST", url: "/events", payload: event }))
        .statusCode,
    ).toBe(202);
    expect(
      (await app.inject({ method: "POST", url: "/events", payload: event }))
        .statusCode,
    ).toBe(409);

    const malformed = await app.inject({
      method: "POST",
      url: "/events",
      payload: {
        ...event,
        recordVersion: 0,
        payload: { ...event.payload, email: "bad" },
      },
    });
    expect(malformed.statusCode).toBe(400);

    const oversized = await app.inject({
      method: "POST",
      url: "/events",
      payload: {
        ...event,
        eventId: "large",
        payload: { ...event.payload, name: "x".repeat(1_000) },
      },
    });
    expect(oversized.statusCode).toBe(413);
  });
});

function eventPayload() {
  return {
    eventId: "event-1",
    recordId: "record-1",
    recordVersion: 1,
    type: "upsert",
    payload: { name: "Ada", email: "ada@example.com", status: "active" },
  };
}

function testConfig(): PipelineConfig {
  return {
    databasePath: ":memory:",
    queueCapacity: 10,
    batchSize: 1,
    maxBatchSize: 5,
    maxBatchWaitMs: 5,
    workers: 1,
    targetConcurrency: 1,
    targetTimeoutMs: 100,
    maxAttempts: 2,
    retryBaseMs: 1,
    retryMaxMs: 10,
    retryCapacity: 10,
    payloadLimitBytes: 256,
  };
}
