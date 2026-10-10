import { abortError } from "../abort.js";
import type { PipelineConfig } from "../config.js";
import type {
  MigrationEvent,
  PipelineStatus,
  SourceEventInput,
  WorkItem,
} from "../domain.js";
import type {
  DlqRepository,
  SourceRepository,
} from "../storage/repositories.js";
import { BoundedQueue } from "../structures/bounded-queue.js";
import { takeBatch } from "./batcher.js";
import {
  CheckpointPersistenceError,
  type CheckpointTracker,
} from "./checkpoint.js";
import { RetryCapacityError, RetryScheduler, retryDelayMs } from "./retry.js";
import { RetryableTargetError, type TargetAdapter } from "./target-adapter.js";
import { NonRetryableError, transform } from "./transformer.js";

export type AdmissionResult =
  | { accepted: true; event: MigrationEvent }
  | { accepted: false; reason: "saturated" };

export interface MigrationPipelineOptions {
  config: PipelineConfig;
  source: SourceRepository;
  target: TargetAdapter;
  checkpoint: CheckpointTracker;
  dlq: DlqRepository;
  random?: () => number;
}

export class MigrationPipeline {
  private readonly config: PipelineConfig;
  private readonly source: SourceRepository;
  private readonly target: TargetAdapter;
  private readonly checkpoint: CheckpointTracker;
  private readonly dlq: DlqRepository;
  private readonly queue: BoundedQueue<WorkItem>;
  private readonly retryScheduler: RetryScheduler;
  private readonly random: () => number;
  private readonly idleWaiters = new Set<() => void>();
  private controller: AbortController | undefined;
  private workerPromises: Promise<void>[] = [];
  private retryPromise: Promise<void> | undefined;
  private activeWorkerCount = 0;
  private state: PipelineStatus["state"] = "stopped";

  constructor(options: MigrationPipelineOptions) {
    this.config = options.config;
    this.source = options.source;
    this.target = options.target;
    this.checkpoint = options.checkpoint;
    this.dlq = options.dlq;
    this.queue = new BoundedQueue(options.config.queueCapacity);
    this.retryScheduler = new RetryScheduler(options.config.retryCapacity);
    this.random = options.random ?? Math.random;
  }

  async start(): Promise<void> {
    if (this.state !== "stopped") {
      return;
    }
    this.state = "running";
    const controller = new AbortController();
    this.controller = controller;
    this.workerPromises = Array.from({ length: this.config.workers }, () =>
      this.workerLoop(controller.signal),
    );
    this.retryPromise = this.retryLoop(controller.signal);
    await this.replay();
  }

  admit(input: SourceEventInput): AdmissionResult {
    this.assertRunning();
    if (this.queue.isFull) {
      return { accepted: false, reason: "saturated" };
    }

    const event = this.source.append(input);
    if (!this.queue.tryEnqueue({ event, attempt: 1 })) {
      throw new Error("queue capacity changed during serialized admission");
    }
    this.notifyStateChange();
    return { accepted: true, event };
  }

  async replay(): Promise<void> {
    let position = this.checkpoint.position;
    while (true) {
      const events = this.source.eventsAfter(
        position,
        this.config.queueCapacity,
      );
      if (events.length === 0) {
        return;
      }
      for (const event of events) {
        await this.queue.enqueue({ event, attempt: 1 });
        position = event.position;
      }
    }
  }

  async stop(options: { drain: boolean; timeoutMs?: number }): Promise<void> {
    if (this.state === "stopped") {
      return;
    }
    if (options.drain) {
      await this.awaitIdle(options.timeoutMs ?? 5_000);
    }

    this.controller?.abort(new Error("pipeline stopped"));
    this.retryScheduler.close();
    this.queue.close();
    await Promise.allSettled([
      ...this.workerPromises,
      ...(this.retryPromise === undefined ? [] : [this.retryPromise]),
    ]);
    this.workerPromises = [];
    this.retryPromise = undefined;
    this.state = "stopped";
    this.notifyStateChange();
  }

  status(): PipelineStatus {
    const sourcePosition = this.source.maxPosition();
    return {
      state: this.state,
      queueDepth: this.queue.size,
      activeWorkers: this.activeWorkerCount,
      scheduledRetries: this.retryScheduler.size,
      checkpointPosition: this.checkpoint.position,
      sourcePosition,
      checkpointLag: sourcePosition - this.checkpoint.position,
    };
  }

  async awaitIdle(timeoutMs: number): Promise<void> {
    if (this.isIdle()) {
      return;
    }
    await new Promise<void>((resolve, reject) => {
      const done = (): void => {
        clearTimeout(timer);
        this.idleWaiters.delete(done);
        resolve();
      };
      const timer = setTimeout(() => {
        this.idleWaiters.delete(done);
        reject(new Error(`pipeline did not become idle within ${timeoutMs}ms`));
      }, timeoutMs);
      this.idleWaiters.add(done);
    });
  }

  private async workerLoop(signal: AbortSignal): Promise<void> {
    try {
      while (!signal.aborted) {
        const batch = await takeBatch(this.queue, {
          batchSize: this.config.batchSize,
          maxBatchSize: this.config.maxBatchSize,
          maxWaitMs: this.config.maxBatchWaitMs,
          signal,
        });
        if (batch.length === 0) {
          return;
        }

        this.activeWorkerCount += 1;
        this.notifyStateChange();
        try {
          await Promise.all(
            batch.map((item) => this.processItem(item, signal)),
          );
        } finally {
          this.activeWorkerCount -= 1;
          this.notifyStateChange();
        }
      }
    } catch (error) {
      if (!signal.aborted) {
        this.state = "degraded";
        throw error;
      }
    }
  }

  private async processItem(
    item: WorkItem,
    signal: AbortSignal,
  ): Promise<void> {
    try {
      const record = transform(item.event);
      await this.target.write(record, item.attempt, signal);
      await this.checkpoint.markTerminal(item.event.position);
    } catch (error) {
      if (signal.aborted) {
        throw abortError(signal);
      }
      const failure =
        error instanceof Error ? error : new Error("unknown failure");
      if (
        failure instanceof RetryableTargetError ||
        failure instanceof CheckpointPersistenceError
      ) {
        if (item.attempt < this.config.maxAttempts) {
          const retry: WorkItem = {
            event: item.event,
            attempt: item.attempt + 1,
          };
          const dueAt =
            Date.now() +
            retryDelayMs(
              item.attempt,
              this.config.retryBaseMs,
              this.config.retryMaxMs,
              this.random,
            );
          try {
            this.retryScheduler.schedule(retry, dueAt);
            this.notifyStateChange();
            return;
          } catch (scheduleError) {
            if (!(scheduleError instanceof RetryCapacityError)) {
              throw scheduleError;
            }
          }
        }
        await this.deadLetter(item, "retry_exhausted", failure);
        return;
      }

      await this.deadLetter(
        item,
        failure instanceof NonRetryableError ? "malformed" : "non_retryable",
        failure,
      );
    }
  }

  private async deadLetter(
    item: WorkItem,
    failureKind: "malformed" | "non_retryable" | "retry_exhausted",
    failure: Error,
  ): Promise<void> {
    this.dlq.add({
      position: item.event.position,
      eventId: item.event.eventId,
      recordId: item.event.recordId,
      eventJson: JSON.stringify(item.event),
      failureKind,
      failureMessage: failure.message,
      attempts: item.attempt,
      failedAt: new Date().toISOString(),
    });
    await this.checkpoint.markTerminal(item.event.position);
  }

  private async retryLoop(signal: AbortSignal): Promise<void> {
    try {
      while (!signal.aborted) {
        const item = await this.retryScheduler.takeDue(signal);
        if (item === undefined) {
          return;
        }
        await this.queue.enqueue(item);
        this.notifyStateChange();
      }
    } catch (error) {
      if (!signal.aborted) {
        this.state = "degraded";
        throw error;
      }
    }
  }

  private isIdle(): boolean {
    return (
      this.queue.size === 0 &&
      this.activeWorkerCount === 0 &&
      this.retryScheduler.size === 0 &&
      this.checkpoint.position >= this.source.maxPosition()
    );
  }

  private notifyStateChange(): void {
    if (!this.isIdle()) {
      return;
    }
    for (const waiter of [...this.idleWaiters]) {
      waiter();
    }
  }

  private assertRunning(): void {
    if (this.state !== "running") {
      throw new Error("pipeline is not running");
    }
  }
}
