import { afterEach, describe, expect, jest, test } from "@jest/globals";

import {
  createMetrics,
  startOperationalSummaries,
  type SummaryLogger,
} from "../src/observability.js";

afterEach(() => {
  jest.useRealTimers();
});

describe("pipeline observability", () => {
  test("renders required counters, gauges, and latency histograms", async () => {
    const metrics = createMetrics();
    metrics.onReceived();
    metrics.onProcessed("applied", 25);
    metrics.onRetry();
    metrics.onFailure("retryable");
    metrics.onDeadLetter();
    metrics.onBatch(40);
    metrics.updateStatus({
      state: "running",
      queueDepth: 7,
      activeWorkers: 2,
      scheduledRetries: 1,
      checkpointPosition: 12,
      sourcePosition: 15,
      checkpointLag: 3,
    });
    metrics.onReconciliation(false);

    const output = await metrics.render();

    expect(output).toContain("migration_records_received_total 1");
    expect(output).toContain(
      'migration_records_processed_total{outcome="applied"} 1',
    );
    expect(output).toContain("migration_records_retried_total 1");
    expect(output).toContain("migration_records_dead_lettered_total 1");
    expect(output).toContain("migration_queue_depth 7");
    expect(output).toContain("migration_worker_count 2");
    expect(output).toContain("migration_checkpoint_position 12");
    expect(output).toContain("migration_checkpoint_lag 3");
    expect(output).toContain("migration_processing_latency_seconds_count 1");
    expect(output).toContain("migration_batch_latency_seconds_count 1");
    expect(output).toContain("migration_reconciliation_failures_total 1");
  });

  test("prints periodic bounded operational summaries", async () => {
    jest.useFakeTimers({ now: 0 });
    const logger = new CapturingLogger();
    const stop = startOperationalSummaries({
      intervalMs: 1_000,
      logger,
      status: () => ({
        state: "running",
        queueDepth: 4,
        activeWorkers: 2,
        scheduledRetries: 1,
        checkpointPosition: 8,
        sourcePosition: 10,
        checkpointLag: 2,
      }),
    });

    await jest.advanceTimersByTimeAsync(1_000);
    stop();

    expect(logger.entries).toHaveLength(1);
    expect(logger.entries[0]).toMatchObject({
      queueDepth: 4,
      activeWorkers: 2,
      scheduledRetries: 1,
      checkpointPosition: 8,
      checkpointLag: 2,
    });
  });
});

class CapturingLogger implements SummaryLogger {
  readonly entries: Record<string, unknown>[] = [];

  info(data: Record<string, unknown>): void {
    this.entries.push(data);
  }
}
