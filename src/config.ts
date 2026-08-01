import { MachineConfig } from './domain/machine';

export interface ServerConfig extends MachineConfig {
  port: number;
  dbPath: string;
  reaperIntervalMs: number;
  /** 测试钩子:首个 POST /v1/commands 在提交落库后、响应前直接退出进程 */
  crashAfterSubmitCommit: boolean;
}

function int(env: string | undefined, fallback: number): number {
  const n = env === undefined ? NaN : Number(env);
  return Number.isFinite(n) && n > 0 ? n : fallback;
}

export function loadConfig(env: NodeJS.ProcessEnv = process.env): ServerConfig {
  return {
    port: int(env.PORT, 8080),
    dbPath: env.DB_PATH ?? './dispatch.db',
    leaseTtlMs: int(env.LEASE_TTL_MS, 15_000),
    maxAttempts: int(env.MAX_ATTEMPTS, 5),
    retryBackoffMs: int(env.RETRY_BACKOFF_MS, 2_000),
    ownershipLeaseTtlMs: int(env.OWNERSHIP_LEASE_TTL_MS, 10_000),
    reaperIntervalMs: int(env.REAPER_INTERVAL_MS, 1_000),
    crashAfterSubmitCommit: env.CRASH_AFTER_SUBMIT_COMMIT === '1',
  };
}
