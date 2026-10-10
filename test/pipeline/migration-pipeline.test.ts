import { afterEach, beforeEach, describe, expect, test } from "@jest/globals";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";

import type { PipelineConfig } from "../../src/config.js";
import {
  CheckpointTracker,
  type CheckpointStore,
} from "../../src/pipeline/checkpoint.js";
import { MigrationPipeline } from "../../src/pipeline/migration-pipeline.js";
import {
  TargetAdapter,
  TargetUnavailableError,
  type FaultPolicy,
} from "../../src/pipeline/target-adapter.js";
import { openDatabase } from "../../src/storage/database.js";
import {
  CheckpointRepository,
  DlqRepository,
  DuplicateEventError,
  SourceRepository,
  TargetRepository,
} from "../../src/storage/repositories.js";

describe("MigrationPipeline", () => {
  let directory: string;
  let database: ReturnType<typeof openDatabase>;
  let source: SourceRepository;
  let target: TargetRepository;
  let checkpoints: CheckpointRepository;
  let dlq: DlqRepository;
  let pipeline: MigrationPipeline | undefined;

  beforeEach(() => {
    directory = mkdtempSync(join(tmpdir(), "migration-pipeline-"));
    database = openDatabase(join(directory, "migration.sqlite"));
    source = new SourceRepository(database);
    target = new TargetRepository(database);
    checkpoints = new CheckpointRepository(database);
    dlq = new DlqRepository(database);
  });

  afterEach(async () => {
    await pipeline?.stop({ drain: false });
    database.close();
    rmSync(directory, { recursive: true, force: true });
  });

  test("migrates an admitted event and advances its checkpoint", async () => {
    pipeline = createPipeline();
    await pipeline.start();

    const admission = pipeline.admit({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: " Ada ", email: "ADA@example.com", status: "active" },
    });

    expect(admission).toMatchObject({ accepted: true });
    await pipeline.awaitIdle(2_000);
    expect(target.getRecord("record-1")).toMatchObject({
      name: "Ada",
      email: "ada@example.com",
      recordVersion: 1,
    });
    expect(checkpoints.get()).toBe(1);
    expect(pipeline.status()).toMatchObject({
      queueDepth: 0,
      activeWorkers: 0,
      scheduledRetries: 0,
      checkpointPosition: 1,
      checkpointLag: 0,
    });
  });

  test("retries a temporary outage and checkpoints after recovery", async () => {
    const faultPolicy = new FailFirstPolicy(2);
    pipeline = createPipeline({ faultPolicy });
    await pipeline.start();

    pipeline.admit({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: "Ada", email: "ada@example.com", status: "active" },
    });

    await pipeline.awaitIdle(2_000);
    expect(faultPolicy.attempts).toBe(3);
    expect(target.getRecord("record-1")).toBeDefined();
    expect(dlq.count()).toBe(0);
    expect(checkpoints.get()).toBe(1);
  });

  test("rejects a duplicate event ID while preserving the first event", async () => {
    pipeline = createPipeline();
    await pipeline.start();
    const input = {
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert" as const,
      payload: {
        name: "Ada",
        email: "ada@example.com",
        status: "active" as const,
      },
    };

    pipeline.admit(input);
    expect(() => pipeline?.admit(input)).toThrow(DuplicateEventError);

    await pipeline.awaitIdle(2_000);
    expect(source.maxPosition()).toBe(1);
    expect(target.countRecords()).toBe(1);
  });

  test("does not let an out-of-order older version overwrite the target", async () => {
    pipeline = createPipeline({ config: { workers: 1, batchSize: 2 } });
    await pipeline.start();

    pipeline.admit({
      eventId: "event-2",
      recordId: "record-1",
      recordVersion: 2,
      type: "upsert",
      payload: { name: "New", email: "new@example.com", status: "active" },
    });
    pipeline.admit({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: "Old", email: "old@example.com", status: "inactive" },
    });

    await pipeline.awaitIdle(2_000);
    expect(target.getRecord("record-1")).toMatchObject({
      recordVersion: 2,
      name: "New",
    });
    expect(checkpoints.get()).toBe(2);
  });

  test("dead-letters a retryable failure after bounded attempts", async () => {
    const faultPolicy = new AlwaysUnavailablePolicy();
    pipeline = createPipeline({
      config: { maxAttempts: 2 },
      faultPolicy,
    });
    await pipeline.start();

    pipeline.admit({
      eventId: "event-1",
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: "Ada", email: "ada@example.com", status: "active" },
    });

    await pipeline.awaitIdle(2_000);
    expect(faultPolicy.attempts).toBe(2);
    expect(target.countRecords()).toBe(0);
    expect(dlq.list()).toEqual([
      expect.objectContaining({
        eventId: "event-1",
        failureKind: "retry_exhausted",
        attempts: 2,
      }),
    ]);
    expect(checkpoints.get()).toBe(1);
  });

  test("keeps successful records from a partially failed batch", async () => {
    pipeline = createPipeline({
      config: { workers: 1, batchSize: 3, maxAttempts: 1 },
      faultPolicy: new RecordFailurePolicy("record-2"),
    });
    await pipeline.start();
    for (let index = 1; index <= 3; index += 1) {
      pipeline.admit({
        eventId: `event-${index}`,
        recordId: `record-${index}`,
        recordVersion: 1,
        type: "upsert",
        payload: {
          name: `Record ${index}`,
          email: `record-${index}@example.com`,
          status: "active",
        },
      });
    }

    await pipeline.awaitIdle(2_000);
    expect(target.countRecords()).toBe(2);
    expect(target.getRecord("record-1")).toBeDefined();
    expect(target.getRecord("record-3")).toBeDefined();
    expect(dlq.list()).toEqual([
      expect.objectContaining({ eventId: "event-2", attempts: 1 }),
    ]);
    expect(checkpoints.get()).toBe(3);
  });

  test("rejects new admission when a slow target fills the bounded queue", async () => {
    const faultPolicy = new BlockingOncePolicy();
    pipeline = createPipeline({
      config: { workers: 1, batchSize: 1, queueCapacity: 1 },
      faultPolicy,
    });
    await pipeline.start();

    expect(pipeline.admit(eventInput(1))).toMatchObject({ accepted: true });
    await faultPolicy.entered;
    expect(pipeline.admit(eventInput(2))).toMatchObject({ accepted: true });
    expect(pipeline.admit(eventInput(3))).toEqual({
      accepted: false,
      reason: "saturated",
    });
    expect(source.maxPosition()).toBe(2);

    faultPolicy.release();
    await pipeline.awaitIdle(2_000);
    expect(target.countRecords()).toBe(2);
    expect(checkpoints.get()).toBe(2);
  });

  test("replays unfinished durable work after a worker restart", async () => {
    const faultPolicy = new BlockingForeverPolicy();
    pipeline = createPipeline({ faultPolicy });
    await pipeline.start();
    pipeline.admit(eventInput(1));
    await faultPolicy.entered;

    await pipeline.stop({ drain: false });
    expect(target.countRecords()).toBe(0);
    expect(checkpoints.get()).toBe(0);

    pipeline = createPipeline();
    await pipeline.start();
    await pipeline.awaitIdle(2_000);

    expect(target.getRecord("record-1")).toBeDefined();
    expect(checkpoints.get()).toBe(1);
  });

  test("replays an idempotent target write after checkpoint persistence fails", async () => {
    const checkpointStore = new FailOnceCheckpointStore(checkpoints);
    pipeline = createPipeline({ checkpointStore });
    await pipeline.start();
    pipeline.admit(eventInput(1));

    await pipeline.awaitIdle(2_000);

    expect(checkpointStore.advanceAttempts).toBe(2);
    expect(target.countRecords()).toBe(1);
    expect(dlq.count()).toBe(0);
    expect(checkpoints.get()).toBe(1);
  });

  test("dead-letters overflow instead of growing the retry heap", async () => {
    pipeline = createPipeline({
      config: {
        workers: 1,
        batchSize: 2,
        retryCapacity: 1,
        retryBaseMs: 50,
        retryMaxMs: 50,
      },
      faultPolicy: new FailFirstPerRecordPolicy(),
      random: () => 1,
    });
    await pipeline.start();
    pipeline.admit(eventInput(1));
    pipeline.admit(eventInput(2));

    await pipeline.awaitIdle(2_000);

    expect(target.countRecords()).toBe(1);
    expect(dlq.list()).toEqual([
      expect.objectContaining({
        eventId: "event-2",
        failureKind: "retry_exhausted",
        attempts: 1,
      }),
    ]);
    expect(checkpoints.get()).toBe(2);
  });

  test("dead-letters a malformed durable event during replay", async () => {
    source.append(eventInput(1));
    database
      .prepare(
        "UPDATE source_events SET payload_json = '{}' WHERE position = 1",
      )
      .run();
    pipeline = createPipeline();

    await pipeline.start();
    await pipeline.awaitIdle(2_000);

    expect(target.countRecords()).toBe(0);
    expect(dlq.list()).toEqual([
      expect.objectContaining({
        eventId: "event-1",
        failureKind: "malformed",
        attempts: 1,
      }),
    ]);
    expect(checkpoints.get()).toBe(1);
  });

  function createPipeline(
    options: {
      config?: Partial<PipelineConfig>;
      faultPolicy?: FaultPolicy;
      checkpointStore?: CheckpointStore;
      random?: () => number;
    } = {},
  ): MigrationPipeline {
    const resolved = pipelineConfig(options.config ?? {});
    return new MigrationPipeline({
      config: resolved,
      source,
      target: new TargetAdapter(target, {
        concurrency: resolved.targetConcurrency,
        timeoutMs: resolved.targetTimeoutMs,
        ...(options.faultPolicy === undefined
          ? {}
          : { faultPolicy: options.faultPolicy }),
      }),
      checkpoint: new CheckpointTracker(options.checkpointStore ?? checkpoints),
      dlq,
      random: options.random ?? (() => 0),
    });
  }
});

class FailFirstPolicy implements FaultPolicy {
  attempts = 0;
  private readonly failures: number;

  constructor(failures: number) {
    this.failures = failures;
  }

  beforeWrite(): void {
    this.attempts += 1;
    if (this.attempts <= this.failures) {
      throw new TargetUnavailableError("simulated outage");
    }
  }
}

class AlwaysUnavailablePolicy implements FaultPolicy {
  attempts = 0;

  beforeWrite(): never {
    this.attempts += 1;
    throw new TargetUnavailableError("simulated outage");
  }
}

class RecordFailurePolicy implements FaultPolicy {
  private readonly failedRecordId: string;

  constructor(failedRecordId: string) {
    this.failedRecordId = failedRecordId;
  }

  beforeWrite(record: { recordId: string }): void {
    if (record.recordId === this.failedRecordId) {
      throw new TargetUnavailableError("record-specific failure");
    }
  }
}

class BlockingOncePolicy implements FaultPolicy {
  readonly entered: Promise<void>;
  private enter: (() => void) | undefined;
  private unblock: (() => void) | undefined;
  private blocked = false;

  constructor() {
    this.entered = new Promise<void>((resolve) => {
      this.enter = resolve;
    });
  }

  beforeWrite(): Promise<void> | void {
    if (this.blocked) {
      return;
    }
    this.blocked = true;
    this.enter?.();
    return new Promise<void>((resolve) => {
      this.unblock = resolve;
    });
  }

  release(): void {
    this.unblock?.();
  }
}

class BlockingForeverPolicy implements FaultPolicy {
  readonly entered: Promise<void>;
  private enter: (() => void) | undefined;

  constructor() {
    this.entered = new Promise<void>((resolve) => {
      this.enter = resolve;
    });
  }

  beforeWrite(): Promise<void> {
    this.enter?.();
    return new Promise(() => undefined);
  }
}

class FailOnceCheckpointStore implements CheckpointStore {
  advanceAttempts = 0;
  private readonly delegate: CheckpointRepository;

  constructor(delegate: CheckpointRepository) {
    this.delegate = delegate;
  }

  get(): number {
    return this.delegate.get();
  }

  advance(expectedCurrent: number, next: number): boolean {
    this.advanceAttempts += 1;
    if (this.advanceAttempts === 1) {
      return false;
    }
    return this.delegate.advance(expectedCurrent, next);
  }
}

class FailFirstPerRecordPolicy implements FaultPolicy {
  private readonly attempts = new Map<string, number>();

  beforeWrite(record: { recordId: string }): void {
    const attempt = (this.attempts.get(record.recordId) ?? 0) + 1;
    this.attempts.set(record.recordId, attempt);
    if (attempt === 1) {
      throw new TargetUnavailableError("first attempt fails");
    }
  }
}

function eventInput(index: number) {
  return {
    eventId: `event-${index}`,
    recordId: `record-${index}`,
    recordVersion: 1,
    type: "upsert" as const,
    payload: {
      name: `Record ${index}`,
      email: `record-${index}@example.com`,
      status: "active" as const,
    },
  };
}

function pipelineConfig(overrides: Partial<PipelineConfig>): PipelineConfig {
  return {
    databasePath: ":memory:",
    queueCapacity: 10,
    batchSize: 2,
    maxBatchSize: 5,
    maxBatchWaitMs: 5,
    workers: 2,
    targetConcurrency: 1,
    targetTimeoutMs: 100,
    maxAttempts: 3,
    retryBaseMs: 1,
    retryMaxMs: 10,
    retryCapacity: 10,
    payloadLimitBytes: 65_536,
    ...overrides,
  };
}
