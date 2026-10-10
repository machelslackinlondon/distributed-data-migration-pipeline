import {
  Counter,
  Gauge,
  Histogram,
  Registry,
  collectDefaultMetrics,
} from "@prometheus-io/client";

import type { ApplyOutcome, PipelineStatus } from "./domain.js";

export interface SummaryLogger {
  info(data: Record<string, unknown>, message?: string): void;
}

export interface PipelineMetrics {
  readonly contentType: string;
  onReceived(): void;
  onProcessed(outcome: ApplyOutcome, durationMs: number): void;
  onFailure(kind: string): void;
  onRetry(): void;
  onDeadLetter(): void;
  onBatch(durationMs: number): void;
  onReconciliation(verified: boolean): void;
  updateStatus(status: PipelineStatus): void;
  render(): Promise<string>;
}

export function createMetrics(): PipelineMetrics {
  const registry = new Registry();
  collectDefaultMetrics({ register: registry, prefix: "migration_process_" });

  const received = new Counter({
    name: "migration_records_received_total",
    help: "Source events durably accepted by the pipeline.",
    registers: [registry],
  });
  const processed = new Counter({
    name: "migration_records_processed_total",
    help: "Events successfully processed, including idempotent no-ops.",
    labelNames: ["outcome"] as const,
    registers: [registry],
  });
  const failed = new Counter({
    name: "migration_records_failed_total",
    help: "Event processing failures by classification.",
    labelNames: ["kind"] as const,
    registers: [registry],
  });
  const retried = new Counter({
    name: "migration_records_retried_total",
    help: "Events scheduled for a bounded retry.",
    registers: [registry],
  });
  const deadLettered = new Counter({
    name: "migration_records_dead_lettered_total",
    help: "Events persisted to the dead-letter queue.",
    registers: [registry],
  });
  const queueDepth = gauge(registry, "migration_queue_depth", "Queued events.");
  const workers = gauge(
    registry,
    "migration_worker_count",
    "Workers currently processing a batch.",
  );
  const retryDepth = gauge(
    registry,
    "migration_retry_queue_depth",
    "Events waiting for retry.",
  );
  const checkpoint = gauge(
    registry,
    "migration_checkpoint_position",
    "Highest contiguous terminal source offset.",
  );
  const checkpointLag = gauge(
    registry,
    "migration_checkpoint_lag",
    "Source position minus checkpoint position.",
  );
  const processingLatency = new Histogram({
    name: "migration_processing_latency_seconds",
    help: "End-to-end processing latency for an event attempt.",
    buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
    registers: [registry],
  });
  const batchLatency = new Histogram({
    name: "migration_batch_latency_seconds",
    help: "Worker batch processing latency.",
    buckets: [0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5],
    registers: [registry],
  });
  const reconciliationFailures = new Counter({
    name: "migration_reconciliation_failures_total",
    help: "Reconciliation runs that did not verify the migration.",
    registers: [registry],
  });

  return {
    contentType: registry.contentType,
    onReceived: () => received.inc(),
    onProcessed: (outcome, durationMs) => {
      processed.inc({ outcome });
      processingLatency.observe(durationMs / 1_000);
    },
    onFailure: (kind) => failed.inc({ kind }),
    onRetry: () => retried.inc(),
    onDeadLetter: () => deadLettered.inc(),
    onBatch: (durationMs) => batchLatency.observe(durationMs / 1_000),
    onReconciliation: (verified) => {
      if (!verified) {
        reconciliationFailures.inc();
      }
    },
    updateStatus: (status) => {
      queueDepth.set(status.queueDepth);
      workers.set(status.activeWorkers);
      retryDepth.set(status.scheduledRetries);
      checkpoint.set(status.checkpointPosition);
      checkpointLag.set(status.checkpointLag);
    },
    render: () => registry.metrics(),
  };
}

export function startOperationalSummaries(options: {
  intervalMs: number;
  logger: SummaryLogger;
  status: () => PipelineStatus;
}): () => void {
  const timer = setInterval(() => {
    const status = options.status();
    const memory = process.memoryUsage();
    const cpu = process.cpuUsage();
    options.logger.info(
      {
        ...status,
        residentMemoryBytes: memory.rss,
        heapUsedBytes: memory.heapUsed,
        cpuUserMicros: cpu.user,
        cpuSystemMicros: cpu.system,
      },
      "migration operational summary",
    );
  }, options.intervalMs);
  timer.unref();
  return () => clearInterval(timer);
}

function gauge(registry: Registry, name: string, help: string): Gauge {
  return new Gauge({ name, help, registers: [registry] });
}
