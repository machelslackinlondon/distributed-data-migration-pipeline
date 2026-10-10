import { loadConfig } from "./config.js";
import { createRuntime } from "./runtime.js";

async function main(): Promise<void> {
  const config = loadConfig();
  const runtime = await createRuntime(config, { logger: true });
  const host = process.env.HOST ?? "0.0.0.0";
  const port = parsePort(process.env.PORT);
  await runtime.app.listen({ host, port });

  let shuttingDown = false;
  const shutdown = async (signal: string): Promise<void> => {
    if (shuttingDown) {
      return;
    }
    shuttingDown = true;
    runtime.app.log.info({ signal }, "graceful shutdown started");
    try {
      await runtime.close({ drain: true, timeoutMs: 10_000 });
    } catch (error) {
      runtime.app.log.error({ error }, "graceful shutdown failed");
      process.exitCode = 1;
    }
  };

  process.once("SIGINT", () => {
    void shutdown("SIGINT");
  });
  process.once("SIGTERM", () => {
    void shutdown("SIGTERM");
  });
}

function parsePort(raw: string | undefined): number {
  const port = raw === undefined ? 3_000 : Number(raw);
  if (!Number.isSafeInteger(port) || port <= 0 || port > 65_535) {
    throw new Error("PORT must be an integer between 1 and 65535");
  }
  return port;
}

void main().catch((error: unknown) => {
  console.error(error);
  process.exitCode = 1;
});
