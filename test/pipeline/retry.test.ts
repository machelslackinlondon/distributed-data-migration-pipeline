import { afterEach, describe, expect, jest, test } from "@jest/globals";

import type { WorkItem } from "../../src/domain.js";
import {
  RetryCapacityError,
  RetryScheduler,
  retryDelayMs,
} from "../../src/pipeline/retry.js";

afterEach(() => {
  jest.useRealTimers();
});

describe("retryDelayMs", () => {
  test("uses exponential full jitter capped at the maximum delay", () => {
    const midpoint = (): number => 0.5;

    expect(retryDelayMs(1, 100, 1_000, midpoint)).toBe(50);
    expect(retryDelayMs(4, 100, 1_000, midpoint)).toBe(400);
    expect(retryDelayMs(8, 100, 1_000, midpoint)).toBe(500);
  });

  test("keeps jitter within zero and the capped delay", () => {
    expect(retryDelayMs(3, 100, 1_000, () => 0)).toBe(0);
    expect(retryDelayMs(3, 100, 1_000, () => 0.999)).toBe(399);
  });
});

describe("RetryScheduler", () => {
  test("releases work by due time and preserves ties", async () => {
    jest.useFakeTimers({ now: 0 });
    const scheduler = new RetryScheduler(3);
    scheduler.schedule(workItem("late"), 30);
    scheduler.schedule(workItem("first"), 10);
    scheduler.schedule(workItem("second"), 10);

    const first = scheduler.takeDue();
    await jest.advanceTimersByTimeAsync(10);
    await expect(first).resolves.toMatchObject({ event: { eventId: "first" } });
    await expect(scheduler.takeDue()).resolves.toMatchObject({
      event: { eventId: "second" },
    });

    const last = scheduler.takeDue();
    await jest.advanceTimersByTimeAsync(20);
    await expect(last).resolves.toMatchObject({ event: { eventId: "late" } });
  });

  test("rejects work beyond its fixed capacity", () => {
    const scheduler = new RetryScheduler(1);
    scheduler.schedule(workItem("first"), 10);

    expect(() => scheduler.schedule(workItem("overflow"), 20)).toThrow(
      RetryCapacityError,
    );
    expect(scheduler.size).toBe(1);
  });

  test("close releases a consumer waiting on an empty scheduler", async () => {
    const scheduler = new RetryScheduler(1);
    const waiting = scheduler.takeDue();

    scheduler.close();

    await expect(waiting).resolves.toBeUndefined();
  });

  test("normalizes a non-Error abort reason", async () => {
    const scheduler = new RetryScheduler(1);
    const controller = new AbortController();
    const waiting = scheduler.takeDue(controller.signal);

    controller.abort("stop");

    await expect(waiting).rejects.toThrow("operation aborted");
  });
});

function workItem(eventId: string): WorkItem {
  return {
    attempt: 1,
    event: {
      position: 1,
      eventId,
      recordId: "record-1",
      recordVersion: 1,
      type: "upsert",
      payload: { name: "Ada", email: "ada@example.com", status: "active" },
      createdAt: "2026-10-10T12:00:00.000Z",
    },
  };
}
