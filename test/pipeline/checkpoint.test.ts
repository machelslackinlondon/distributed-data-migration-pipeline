import { describe, expect, test } from "@jest/globals";

import {
  CheckpointPersistenceError,
  CheckpointTracker,
  type CheckpointStore,
} from "../../src/pipeline/checkpoint.js";

describe("CheckpointTracker", () => {
  test("does not advance across a gap and collapses a completed prefix", async () => {
    const store = new MemoryCheckpointStore();
    const tracker = new CheckpointTracker(store);

    await expect(tracker.markTerminal(3)).resolves.toBe(0);
    await expect(tracker.markTerminal(1)).resolves.toBe(1);
    await expect(tracker.markTerminal(2)).resolves.toBe(3);

    expect(store.position).toBe(3);
    expect(tracker.position).toBe(3);
  });

  test("does not publish a position when persistence fails", async () => {
    const store = new MemoryCheckpointStore();
    const tracker = new CheckpointTracker(store);
    store.failNext = true;

    await expect(tracker.markTerminal(1)).rejects.toBeInstanceOf(
      CheckpointPersistenceError,
    );
    expect(tracker.position).toBe(0);
    expect(store.position).toBe(0);

    await expect(tracker.markTerminal(1)).resolves.toBe(1);
    expect(tracker.position).toBe(1);
  });

  test("serializes concurrent completions without losing offsets", async () => {
    const store = new MemoryCheckpointStore();
    const tracker = new CheckpointTracker(store);

    await Promise.all([
      tracker.markTerminal(2),
      tracker.markTerminal(1),
      tracker.markTerminal(4),
      tracker.markTerminal(3),
    ]);

    expect(tracker.position).toBe(4);
    expect(store.position).toBe(4);
  });
});

class MemoryCheckpointStore implements CheckpointStore {
  position = 0;
  failNext = false;

  get(): number {
    return this.position;
  }

  advance(expectedCurrent: number, next: number): boolean {
    if (this.failNext) {
      this.failNext = false;
      return false;
    }
    if (this.position !== expectedCurrent || next <= this.position) {
      return false;
    }
    this.position = next;
    return true;
  }
}
