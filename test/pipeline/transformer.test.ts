import { describe, expect, test } from "@jest/globals";

import type { MigrationEvent } from "../../src/domain.js";
import {
  NonRetryableError,
  canonicalHash,
  transform,
} from "../../src/pipeline/transformer.js";

describe("transform", () => {
  test("normalizes source fields and produces a deterministic target record", () => {
    const result = transform(
      migrationEvent({
        name: "  Ada  ",
        email: "ADA@EXAMPLE.COM ",
        status: "active",
      }),
    );

    expect(result).toEqual({
      recordId: "record-1",
      recordVersion: 2,
      name: "Ada",
      email: "ada@example.com",
      status: "active",
      contentHash:
        "9f635d72e0579a90611bc99f2df263c6fa834788358d39ebae31f664e8ced2cb",
      lastEventId: "event-2",
      updatedAt: "2026-10-10T12:00:00.000Z",
    });
  });

  test("canonical hashing is independent of object key order", () => {
    expect(
      canonicalHash({
        status: "active",
        name: "Ada",
        email: "ada@example.com",
      }),
    ).toBe(
      canonicalHash({
        email: "ada@example.com",
        status: "active",
        name: "Ada",
      }),
    );
  });

  test("classifies malformed durable payloads as non-retryable", () => {
    const malformed = migrationEvent({
      name: "",
      email: "not-an-email",
      status: "active",
    });

    expect(() => transform(malformed)).toThrow(NonRetryableError);
  });
});

function migrationEvent(payload: MigrationEvent["payload"]): MigrationEvent {
  return {
    position: 2,
    eventId: "event-2",
    recordId: "record-1",
    recordVersion: 2,
    type: "upsert",
    payload,
    createdAt: "2026-10-10T12:00:00.000Z",
  };
}
