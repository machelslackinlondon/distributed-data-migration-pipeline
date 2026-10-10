import { describe, expect, test } from "@jest/globals";

import type { AdmissionResult } from "../src/pipeline/migration-pipeline.js";
import { generateEvents } from "../src/generator.js";

describe("generateEvents", () => {
  test("generates deterministic record versions and honors the requested count", async () => {
    const accepted: string[] = [];
    const result = await generateEvents({
      count: 5,
      recordCount: 2,
      ratePerSecond: 10_000,
      admit: (input) => {
        accepted.push(`${input.recordId}:v${input.recordVersion}`);
        return acceptedResult(accepted.length);
      },
      eventId: (index) => `event-${index}`,
      sleep: () => Promise.resolve(),
      now: () => 10,
    });

    expect(accepted).toEqual([
      "record-1:v1",
      "record-2:v1",
      "record-1:v2",
      "record-2:v2",
      "record-1:v3",
    ]);
    expect(result).toEqual({ generated: 5, throttled: 0, durationMs: 0 });
  });

  test("retries the same event after bounded-queue backpressure", async () => {
    const attemptedEventIds: string[] = [];
    const sleeps: number[] = [];
    let calls = 0;

    const result = await generateEvents({
      count: 2,
      recordCount: 2,
      ratePerSecond: 1_000,
      admit: (input) => {
        calls += 1;
        attemptedEventIds.push(input.eventId);
        return calls <= 2
          ? { accepted: false, reason: "saturated" }
          : acceptedResult(calls);
      },
      eventId: (index) => `event-${index}`,
      sleep: (milliseconds) => {
        sleeps.push(milliseconds);
        return Promise.resolve();
      },
      now: () => 0,
    });

    expect(attemptedEventIds).toEqual([
      "event-1",
      "event-1",
      "event-1",
      "event-2",
    ]);
    expect(result).toMatchObject({ generated: 2, throttled: 2 });
    expect(sleeps.filter((value) => value === 10)).toHaveLength(2);
  });

  test("stops before generating when already aborted", async () => {
    const controller = new AbortController();
    controller.abort(new Error("generation cancelled"));

    await expect(
      generateEvents({
        count: 1,
        recordCount: 1,
        ratePerSecond: 1,
        admit: () => acceptedResult(1),
        signal: controller.signal,
      }),
    ).rejects.toThrow("generation cancelled");
  });
});

function acceptedResult(position: number): AdmissionResult {
  return {
    accepted: true,
    event: {
      position,
      eventId: `event-${position}`,
      recordId: `record-${position}`,
      recordVersion: 1,
      type: "upsert",
      payload: {
        name: "Record",
        email: "record@example.com",
        status: "active",
      },
      createdAt: "2026-10-10T12:00:00.000Z",
    },
  };
}
