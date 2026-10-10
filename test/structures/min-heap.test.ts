import { describe, expect, test } from "@jest/globals";

import { MinHeap } from "../../src/structures/min-heap.js";

interface ScheduledValue {
  id: string;
  dueAt: number;
}

describe("MinHeap", () => {
  test("removes values in comparator order", () => {
    const heap = new MinHeap<ScheduledValue>(
      (left, right) => left.dueAt - right.dueAt,
    );

    heap.push({ id: "last", dueAt: 30 });
    heap.push({ id: "first", dueAt: 10 });
    heap.push({ id: "middle", dueAt: 20 });

    expect(heap.peek()?.id).toBe("first");
    expect([heap.pop()?.id, heap.pop()?.id, heap.pop()?.id]).toEqual([
      "first",
      "middle",
      "last",
    ]);
    expect(heap.pop()).toBeUndefined();
  });

  test("preserves insertion order when priorities are equal", () => {
    const heap = new MinHeap<ScheduledValue>(
      (left, right) => left.dueAt - right.dueAt,
    );

    heap.push({ id: "a", dueAt: 10 });
    heap.push({ id: "b", dueAt: 10 });
    heap.push({ id: "c", dueAt: 10 });

    expect([heap.pop()?.id, heap.pop()?.id, heap.pop()?.id]).toEqual([
      "a",
      "b",
      "c",
    ]);
  });
});
