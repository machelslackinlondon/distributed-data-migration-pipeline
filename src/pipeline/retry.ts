import type { WorkItem } from "../domain.js";
import { abortError } from "../abort.js";
import { MinHeap } from "../structures/min-heap.js";

export class RetryCapacityError extends Error {
  constructor() {
    super("retry scheduler capacity exhausted");
    this.name = "RetryCapacityError";
  }
}

interface ScheduledRetry {
  item: WorkItem;
  dueAt: number;
}

export function retryDelayMs(
  attempt: number,
  baseMs: number,
  maxMs: number,
  random: () => number = Math.random,
): number {
  if (!Number.isSafeInteger(attempt) || attempt <= 0) {
    throw new Error("attempt must be a positive integer");
  }
  if (baseMs <= 0 || maxMs <= 0 || baseMs > maxMs) {
    throw new Error(
      "retry delays must be positive and base must not exceed max",
    );
  }
  const sample = random();
  if (sample < 0 || sample > 1 || !Number.isFinite(sample)) {
    throw new Error("random sample must be between zero and one");
  }

  const exponent = Math.min(attempt - 1, 52);
  const cappedDelay = Math.min(maxMs, baseMs * 2 ** exponent);
  return Math.floor(cappedDelay * sample);
}

export class RetryScheduler {
  private readonly capacity: number;
  private readonly heap = new MinHeap<ScheduledRetry>(
    (left, right) => left.dueAt - right.dueAt,
  );
  private readonly waiters = new Set<() => void>();
  private closed = false;

  constructor(capacity: number) {
    if (!Number.isSafeInteger(capacity) || capacity <= 0) {
      throw new Error("retry capacity must be a positive integer");
    }
    this.capacity = capacity;
  }

  get size(): number {
    return this.heap.size;
  }

  schedule(item: WorkItem, dueAt: number): void {
    if (this.closed) {
      throw new Error("retry scheduler is closed");
    }
    if (this.heap.size >= this.capacity) {
      throw new RetryCapacityError();
    }
    if (!Number.isFinite(dueAt)) {
      throw new Error("retry due time must be finite");
    }
    this.heap.push({ item, dueAt });
    this.wake();
  }

  async takeDue(signal?: AbortSignal): Promise<WorkItem | undefined> {
    while (!this.closed) {
      const next = this.heap.peek();
      if (next === undefined) {
        await this.waitForChange(signal);
        continue;
      }

      const remainingMs = next.dueAt - Date.now();
      if (remainingMs <= 0) {
        return this.heap.pop()?.item;
      }
      await this.waitForChange(signal, remainingMs);
    }
    return undefined;
  }

  close(): void {
    if (this.closed) {
      return;
    }
    this.closed = true;
    this.wake();
  }

  private waitForChange(
    signal?: AbortSignal,
    timeoutMs?: number,
  ): Promise<void> {
    if (signal?.aborted === true) {
      return Promise.reject(abortError(signal));
    }

    return new Promise<void>((resolve, reject) => {
      let timer: NodeJS.Timeout | undefined;
      const settle = (): void => {
        this.waiters.delete(settle);
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        signal?.removeEventListener("abort", onAbort);
        resolve();
      };
      const onAbort = (): void => {
        this.waiters.delete(settle);
        if (timer !== undefined) {
          clearTimeout(timer);
        }
        reject(abortError(signal));
      };

      this.waiters.add(settle);
      signal?.addEventListener("abort", onAbort, { once: true });
      if (timeoutMs !== undefined) {
        timer = setTimeout(settle, timeoutMs);
      }
    });
  }

  private wake(): void {
    for (const waiter of [...this.waiters]) {
      waiter();
    }
  }
}
