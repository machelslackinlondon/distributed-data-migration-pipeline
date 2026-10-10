export interface PipelineConfig {
  databasePath: string;
  queueCapacity: number;
  batchSize: number;
  maxBatchSize: number;
  maxBatchWaitMs: number;
  workers: number;
  targetConcurrency: number;
  targetTimeoutMs: number;
  maxAttempts: number;
  retryBaseMs: number;
  retryMaxMs: number;
  retryCapacity: number;
  payloadLimitBytes: number;
}

const defaults = {
  DATABASE_PATH: "./data/migration.sqlite",
  QUEUE_CAPACITY: 1_000,
  BATCH_SIZE: 50,
  MAX_BATCH_SIZE: 500,
  MAX_BATCH_WAIT_MS: 100,
  WORKERS: 4,
  TARGET_CONCURRENCY: 2,
  TARGET_TIMEOUT_MS: 2_000,
  MAX_ATTEMPTS: 5,
  RETRY_BASE_MS: 100,
  RETRY_MAX_MS: 10_000,
  RETRY_CAPACITY: 2_000,
  PAYLOAD_LIMIT_BYTES: 65_536,
} as const;

function positiveInteger(
  env: NodeJS.ProcessEnv,
  name: Exclude<keyof typeof defaults, "DATABASE_PATH">,
): number {
  const raw = env[name];
  const value = raw === undefined ? defaults[name] : Number(raw);

  if (!Number.isSafeInteger(value) || value <= 0) {
    throw new Error(`${name} must be a positive integer`);
  }

  return value;
}

export function loadConfig(
  env: NodeJS.ProcessEnv = process.env,
): PipelineConfig {
  const config: PipelineConfig = {
    databasePath: env.DATABASE_PATH ?? defaults.DATABASE_PATH,
    queueCapacity: positiveInteger(env, "QUEUE_CAPACITY"),
    batchSize: positiveInteger(env, "BATCH_SIZE"),
    maxBatchSize: positiveInteger(env, "MAX_BATCH_SIZE"),
    maxBatchWaitMs: positiveInteger(env, "MAX_BATCH_WAIT_MS"),
    workers: positiveInteger(env, "WORKERS"),
    targetConcurrency: positiveInteger(env, "TARGET_CONCURRENCY"),
    targetTimeoutMs: positiveInteger(env, "TARGET_TIMEOUT_MS"),
    maxAttempts: positiveInteger(env, "MAX_ATTEMPTS"),
    retryBaseMs: positiveInteger(env, "RETRY_BASE_MS"),
    retryMaxMs: positiveInteger(env, "RETRY_MAX_MS"),
    retryCapacity: positiveInteger(env, "RETRY_CAPACITY"),
    payloadLimitBytes: positiveInteger(env, "PAYLOAD_LIMIT_BYTES"),
  };

  if (config.batchSize > config.maxBatchSize) {
    throw new Error("BATCH_SIZE must not exceed MAX_BATCH_SIZE");
  }
  if (config.targetConcurrency > config.workers) {
    throw new Error("TARGET_CONCURRENCY must not exceed WORKERS");
  }
  if (config.retryBaseMs > config.retryMaxMs) {
    throw new Error("RETRY_BASE_MS must not exceed RETRY_MAX_MS");
  }

  return config;
}
