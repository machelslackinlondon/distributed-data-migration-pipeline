import type Database from "better-sqlite3";

import { buildApp, type AppOptions } from "./app.js";
import type { PipelineConfig } from "./config.js";
import { createMetrics, startOperationalSummaries } from "./observability.js";
import { CheckpointTracker } from "./pipeline/checkpoint.js";
import { MigrationPipeline } from "./pipeline/migration-pipeline.js";
import { TargetAdapter, type FaultPolicy } from "./pipeline/target-adapter.js";
import { reconcile } from "./reconciliation.js";
import { openDatabase } from "./storage/database.js";
import {
  CheckpointRepository,
  DlqRepository,
  SourceRepository,
  TargetRepository,
} from "./storage/repositories.js";

export interface RuntimeOptions {
  logger?: boolean;
  summaries?: boolean;
  summaryIntervalMs?: number;
  reconciliationPageSize?: number;
  faultPolicy?: FaultPolicy;
}

export interface MigrationRuntime {
  app: ReturnType<typeof buildApp>;
  pipeline: MigrationPipeline;
  database: Database.Database;
  source: SourceRepository;
  target: TargetRepository;
  checkpoints: CheckpointRepository;
  dlq: DlqRepository;
  close(options: { drain: boolean; timeoutMs?: number }): Promise<void>;
}

export async function createRuntime(
  config: PipelineConfig,
  options: RuntimeOptions = {},
): Promise<MigrationRuntime> {
  const database = openDatabase(config.databasePath);
  const source = new SourceRepository(database);
  const target = new TargetRepository(database);
  const checkpoints = new CheckpointRepository(database);
  const dlq = new DlqRepository(database);
  const metrics = createMetrics();
  const targetAdapter = new TargetAdapter(target, {
    concurrency: config.targetConcurrency,
    timeoutMs: config.targetTimeoutMs,
    ...(options.faultPolicy === undefined
      ? {}
      : { faultPolicy: options.faultPolicy }),
  });
  const pipeline = new MigrationPipeline({
    config,
    source,
    target: targetAdapter,
    checkpoint: new CheckpointTracker(checkpoints),
    dlq,
    observer: metrics,
  });
  const appOptions: AppOptions = {
    config,
    pipeline,
    metrics,
    runReconciliation: () =>
      reconcile(source, target, dlq, options.reconciliationPageSize ?? 500),
    ...(options.logger === undefined ? {} : { logger: options.logger }),
  };
  const app = buildApp(appOptions);
  await pipeline.start();
  const stopSummaries =
    options.summaries === false
      ? () => undefined
      : startOperationalSummaries({
          intervalMs: options.summaryIntervalMs ?? 10_000,
          logger: app.log,
          status: () => pipeline.status(),
        });
  let closed = false;

  return {
    app,
    pipeline,
    database,
    source,
    target,
    checkpoints,
    dlq,
    close: async (closeOptions) => {
      if (closed) {
        return;
      }
      closed = true;
      stopSummaries();
      await app.close();
      try {
        await pipeline.stop(closeOptions);
      } finally {
        database.close();
      }
    },
  };
}
