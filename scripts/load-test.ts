import autocannon from "autocannon";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { pathToFileURL } from "node:url";

import { abortError } from "../src/abort.js";
import { loadConfig } from "../src/config.js";
import type { TargetRecord } from "../src/domain.js";
import {
  TargetUnavailableError,
  type FaultPolicy,
} from "../src/pipeline/target-adapter.js";
import { createRuntime } from "../src/runtime.js";
import {
  loadProfiles,
  parseLoadMetrics,
  parseProfileName,
  shapeLoadResult,
  type FaultMode,
} from "./load-profiles.js";

async function main(): Promise<void> {
  const profileName = parseProfileName(process.argv.slice(2));
  const profile = loadProfiles[profileName];
  const directory = mkdtempSync(
    join(tmpdir(), `migration-load-${profileName}-`),
  );
  const baseConfig = loadConfig({
    DATABASE_PATH: join(directory, "migration.sqlite"),
  });
  const config = { ...baseConfig, ...profile.config };
  const runtime = await createRuntime(config, {
    faultPolicy: faultPolicy(profile.fault),
    logger: false,
    summaries: false,
  });

  try {
    const address = await runtime.app.listen({ host: "127.0.0.1", port: 0 });
    const startStatus = runtime.pipeline.status();
    const cpuStart = process.cpuUsage();
    let sequence = 0;
    const nextBody = (): string => eventBody((sequence += 1));
    const result = await new Promise<autocannon.Result>((resolve, reject) => {
      const instance = autocannon(
        {
          url: `${address}/events`,
          connections: profile.connections,
          duration: profile.durationSeconds,
          overallRate: profile.overallRate,
          method: "POST",
          headers: { "content-type": "application/json" },
          setupClient: (client) => client.setBody(nextBody()),
        },
        (error, completed) => {
          if (error instanceof Error) {
            reject(error);
            return;
          }
          resolve(completed);
        },
      );
      instance.on("response", (client) => client.setBody(nextBody()));
    });
    await runtime.pipeline.awaitIdle(5_000).catch(() => undefined);
    const metricsResponse = await runtime.app.inject({
      method: "GET",
      url: "/metrics",
    });
    const loadMetrics = parseLoadMetrics(metricsResponse.body);
    const cpu = process.cpuUsage(cpuStart);
    const memory = process.memoryUsage();
    const output = shapeLoadResult({
      profile: profileName,
      requestsPerSecond: result.requests.average,
      latencyP99Ms: result.latency.p99,
      errors: result.errors,
      timeouts: result.timeouts,
      non2xx: result.non2xx,
      durationSeconds: result.duration,
      startStatus,
      endStatus: runtime.pipeline.status(),
      cpuUserMicros: cpu.user,
      cpuSystemMicros: cpu.system,
      residentMemoryBytes: memory.rss,
      targetCount: runtime.target.countRecords(),
      deadLetterCount: runtime.dlq.count(),
      ...loadMetrics,
    });
    console.log(JSON.stringify(output, null, 2));
  } finally {
    await runtime.close({ drain: false });
    rmSync(directory, { recursive: true, force: true });
  }
}

function eventBody(sequence: number): string {
  const recordNumber = (sequence % 1_000) + 1;
  return JSON.stringify({
    eventId: `load-${process.pid}-${sequence}`,
    recordId: `record-${recordNumber}`,
    recordVersion: Math.floor(sequence / 1_000) + 1,
    type: "upsert",
    payload: {
      name: `Record ${recordNumber}`,
      email: `record-${recordNumber}@example.com`,
      status: "active",
    },
  });
}

function faultPolicy(mode: FaultMode): FaultPolicy {
  let attempts = 0;
  return {
    beforeWrite: async (
      _record: TargetRecord,
      _attempt: number,
      signal: AbortSignal,
    ) => {
      attempts += 1;
      if (mode.kind === "delay") {
        await delay(mode.delayMs, signal);
      } else if (mode.kind === "outage") {
        throw new TargetUnavailableError("load-test target outage");
      } else if (mode.kind === "recover" && attempts <= mode.failures) {
        throw new TargetUnavailableError("load-test target recovering");
      }
    },
  };
}

function delay(milliseconds: number, signal: AbortSignal): Promise<void> {
  if (signal.aborted) {
    return Promise.reject(abortError(signal));
  }
  return new Promise<void>((resolve, reject) => {
    const timer = setTimeout(() => {
      signal.removeEventListener("abort", onAbort);
      resolve();
    }, milliseconds);
    const onAbort = (): void => {
      clearTimeout(timer);
      reject(abortError(signal));
    };
    signal.addEventListener("abort", onAbort, { once: true });
  });
}

const entry = process.argv[1];
if (entry !== undefined && import.meta.url === pathToFileURL(entry).href) {
  void main().catch((error: unknown) => {
    console.error(error);
    process.exitCode = 1;
  });
}
