import type { BoundedQueue } from "../structures/bounded-queue.js";
import { abortError } from "../abort.js";

export interface BatchOptions {
  batchSize: number;
  maxBatchSize: number;
  maxWaitMs: number;
  signal?: AbortSignal;
}

class BatchWaitElapsed extends Error {}

export async function takeBatch<T>(
  queue: BoundedQueue<T>,
  options: BatchOptions,
): Promise<T[]> {
  validateOptions(options);
  const limit = Math.min(options.batchSize, options.maxBatchSize);
  const first = await queue.dequeue(options.signal);
  if (first === undefined) {
    return [];
  }

  const batch = [first];
  const deadline = Date.now() + options.maxWaitMs;
  while (batch.length < limit) {
    const remainingMs = deadline - Date.now();
    if (remainingMs <= 0) {
      break;
    }

    const item = await dequeueBefore(queue, remainingMs, options.signal);
    if (item === undefined) {
      break;
    }
    batch.push(item);
  }
  return batch;
}

async function dequeueBefore<T>(
  queue: BoundedQueue<T>,
  waitMs: number,
  signal?: AbortSignal,
): Promise<T | undefined> {
  const controller = new AbortController();
  const timeoutReason = new BatchWaitElapsed();
  const onAbort = (): void => controller.abort(abortError(signal));
  if (signal !== undefined) {
    if (signal.aborted) {
      throw abortError(signal);
    }
    signal.addEventListener("abort", onAbort, { once: true });
  }
  const timer = setTimeout(() => controller.abort(timeoutReason), waitMs);

  try {
    return await queue.dequeue(controller.signal);
  } catch (error) {
    if (error === timeoutReason) {
      return undefined;
    }
    throw error;
  } finally {
    clearTimeout(timer);
    signal?.removeEventListener("abort", onAbort);
  }
}

function validateOptions(options: BatchOptions): void {
  for (const [name, value] of [
    ["batchSize", options.batchSize],
    ["maxBatchSize", options.maxBatchSize],
    ["maxWaitMs", options.maxWaitMs],
  ] as const) {
    if (!Number.isSafeInteger(value) || value <= 0) {
      throw new Error(`${name} must be a positive integer`);
    }
  }
}
