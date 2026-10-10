import { createHash } from "node:crypto";
import { z } from "zod";

import type { MigrationEvent, SourcePayload, TargetRecord } from "../domain.js";

export class NonRetryableError extends Error {
  constructor(message: string, cause?: unknown) {
    super(message, { cause });
    this.name = "NonRetryableError";
  }
}

const payloadSchema = z.object({
  name: z.string().trim().min(1).max(200),
  email: z.string().trim().toLowerCase().pipe(z.email()),
  status: z.enum(["active", "inactive"]),
});

const eventSchema = z.object({
  position: z.int().positive(),
  eventId: z.string().min(1).max(256),
  recordId: z.string().min(1).max(256),
  recordVersion: z.int().positive(),
  type: z.literal("upsert"),
  payload: payloadSchema,
  createdAt: z.iso.datetime(),
});

export function transform(event: MigrationEvent): TargetRecord {
  const parsed = eventSchema.safeParse(event);
  if (!parsed.success) {
    throw new NonRetryableError("malformed migration event", parsed.error);
  }

  const payload: SourcePayload = parsed.data.payload;
  return {
    recordId: parsed.data.recordId,
    recordVersion: parsed.data.recordVersion,
    ...payload,
    contentHash: canonicalHash(payload),
    lastEventId: parsed.data.eventId,
    updatedAt: parsed.data.createdAt,
  };
}

export function canonicalHash(value: unknown): string {
  const canonical = JSON.stringify(canonicalize(value));
  return createHash("sha256").update(canonical).digest("hex");
}

function canonicalize(value: unknown): unknown {
  if (Array.isArray(value)) {
    return value.map(canonicalize);
  }
  if (value !== null && typeof value === "object") {
    const sorted: Record<string, unknown> = {};
    for (const key of Object.keys(value).sort()) {
      sorted[key] = canonicalize((value as Record<string, unknown>)[key]);
    }
    return sorted;
  }
  return value;
}
