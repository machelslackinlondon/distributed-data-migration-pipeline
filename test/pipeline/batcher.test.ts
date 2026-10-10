import { afterEach, describe, expect, jest, test } from "@jest/globals";

import { takeBatch } from "../../src/pipeline/batcher.js";
import { BoundedQueue } from "../../src/structures/bounded-queue.js";

afterEach(() => {
  jest.useRealTimers();
});

describe("takeBatch", () => {
  test("flushes immediately at the configured batch size", async () => {
    const queue = new BoundedQueue<number>(3);
    queue.tryEnqueue(1);
    queue.tryEnqueue(2);
    queue.tryEnqueue(3);

    await expect(
      takeBatch(queue, { batchSize: 3, maxBatchSize: 5, maxWaitMs: 100 }),
    ).resolves.toEqual([1, 2, 3]);
  });

  test("flushes a partial batch at the maximum wait deadline", async () => {
    jest.useFakeTimers({ now: 0 });
    const queue = new BoundedQueue<number>(3);
    queue.tryEnqueue(1);

    const batch = takeBatch(queue, {
      batchSize: 3,
      maxBatchSize: 5,
      maxWaitMs: 100,
    });
    await jest.advanceTimersByTimeAsync(100);

    await expect(batch).resolves.toEqual([1]);
  });

  test("never exceeds the hard maximum batch size", async () => {
    const queue = new BoundedQueue<number>(5);
    for (const value of [1, 2, 3, 4, 5]) {
      queue.tryEnqueue(value);
    }

    await expect(
      takeBatch(queue, { batchSize: 5, maxBatchSize: 3, maxWaitMs: 100 }),
    ).resolves.toEqual([1, 2, 3]);
    expect(queue.size).toBe(2);
  });

  test("aborts while waiting for the first item", async () => {
    const queue = new BoundedQueue<number>(1);
    const controller = new AbortController();
    const batch = takeBatch(queue, {
      batchSize: 1,
      maxBatchSize: 1,
      maxWaitMs: 100,
      signal: controller.signal,
    });

    controller.abort(new Error("worker stopped"));

    await expect(batch).rejects.toThrow("worker stopped");
  });
});
