import { describe, expect, test } from "@jest/globals";

import {
  BoundedQueue,
  QueueClosedError,
} from "../../src/structures/bounded-queue.js";

describe("BoundedQueue", () => {
  test("preserves FIFO order through ring-buffer wraparound", async () => {
    const queue = new BoundedQueue<number>(2);

    expect(queue.tryEnqueue(1)).toBe(true);
    expect(queue.tryEnqueue(2)).toBe(true);
    expect(queue.tryEnqueue(3)).toBe(false);
    await expect(queue.dequeue()).resolves.toBe(1);
    expect(queue.tryEnqueue(3)).toBe(true);

    await expect(queue.dequeue()).resolves.toBe(2);
    await expect(queue.dequeue()).resolves.toBe(3);
    expect(queue.size).toBe(0);
  });

  test("blocks a producer until bounded capacity is available", async () => {
    const queue = new BoundedQueue<number>(1);
    queue.tryEnqueue(1);
    let admitted = false;

    const pending = queue.enqueue(2).then(() => {
      admitted = true;
    });
    await Promise.resolve();
    expect(admitted).toBe(false);

    await expect(queue.dequeue()).resolves.toBe(1);
    await pending;
    expect(admitted).toBe(true);
    await expect(queue.dequeue()).resolves.toBe(2);
  });

  test("delivers directly to a waiting consumer", async () => {
    const queue = new BoundedQueue<number>(1);
    const pending = queue.dequeue();

    expect(queue.tryEnqueue(7)).toBe(true);

    await expect(pending).resolves.toBe(7);
    expect(queue.size).toBe(0);
  });

  test("close resolves waiting consumers and rejects blocked producers", async () => {
    const emptyQueue = new BoundedQueue<number>(1);
    const consumer = emptyQueue.dequeue();

    const fullQueue = new BoundedQueue<number>(1);
    fullQueue.tryEnqueue(1);
    const producer = fullQueue.enqueue(2);

    emptyQueue.close();
    fullQueue.close();

    await expect(consumer).resolves.toBeUndefined();
    await expect(producer).rejects.toBeInstanceOf(QueueClosedError);
    await expect(fullQueue.dequeue()).resolves.toBe(1);
    await expect(fullQueue.dequeue()).resolves.toBeUndefined();
  });

  test("rejects non-positive capacity", () => {
    expect(() => new BoundedQueue(0)).toThrow(
      "queue capacity must be a positive integer",
    );
  });
});
