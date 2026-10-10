import Fastify, { type FastifyInstance } from "fastify";
import { z } from "zod";

import type { PipelineConfig } from "./config.js";
import type { ReconciliationReport } from "./domain.js";
import type { PipelineMetrics } from "./observability.js";
import type { MigrationPipeline } from "./pipeline/migration-pipeline.js";
import { DuplicateEventError } from "./storage/repositories.js";

export interface AppOptions {
  config: PipelineConfig;
  pipeline: MigrationPipeline;
  metrics: PipelineMetrics;
  runReconciliation: () => ReconciliationReport | Promise<ReconciliationReport>;
  logger?: boolean;
}

const eventSchema = z.object({
  eventId: z.string().min(1).max(256),
  recordId: z.string().min(1).max(256),
  recordVersion: z.int().positive(),
  type: z.literal("upsert"),
  payload: z.object({
    name: z.string().trim().min(1).max(200),
    email: z.email(),
    status: z.enum(["active", "inactive"]),
  }),
});

export function buildApp(options: AppOptions): FastifyInstance {
  const app = Fastify({
    bodyLimit: options.config.payloadLimitBytes,
    logger: options.logger ?? false,
  });

  app.post("/events", async (request, reply) => {
    const parsed = eventSchema.safeParse(request.body);
    if (!parsed.success) {
      return reply.code(400).send({
        error: "invalid_event",
        details: parsed.error.issues.map((issue) => ({
          path: issue.path.join("."),
          message: issue.message,
        })),
      });
    }

    try {
      const result = options.pipeline.admit(parsed.data);
      if (!result.accepted) {
        return reply
          .header("Retry-After", "1")
          .code(429)
          .send({ error: "queue_saturated" });
      }
      return reply.code(202).send({
        accepted: true,
        eventId: result.event.eventId,
        position: result.event.position,
      });
    } catch (error) {
      if (error instanceof DuplicateEventError) {
        return reply.code(409).send({ error: "duplicate_event" });
      }
      throw error;
    }
  });

  app.get("/health", async (_request, reply) => {
    const status = options.pipeline.status();
    const ready = status.state === "running";
    return reply.code(ready ? 200 : 503).send({
      status: ready ? "ok" : "degraded",
      ready,
      ...status,
    });
  });

  app.get("/metrics", async (_request, reply) => {
    options.metrics.updateStatus(options.pipeline.status());
    return reply
      .header("Content-Type", options.metrics.contentType)
      .send(await options.metrics.render());
  });

  app.post("/reconciliation", async (_request, reply) => {
    const report = await options.runReconciliation();
    options.metrics.onReconciliation(report.verified);
    return reply.code(200).send(report);
  });

  return app;
}
