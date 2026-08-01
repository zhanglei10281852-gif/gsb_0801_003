import { resolve } from 'node:path';

export interface AppConfig {
  port: number;
  host: string;
  dbPath: string;
  leaseDurationMs: number;
  maxAttempts: number;
  maxAgeMs?: number;
  sweepIntervalMs: number;
  claimBatchSize: number;
  faultInjectionEnabled: boolean;
}

function asInt(name: string, fallback: number): number {
  const raw = process.env[name];
  if (!raw) return fallback;
  const n = Number.parseInt(raw, 10);
  if (!Number.isFinite(n)) throw new Error(`Invalid ${name}: ${raw}`);
  return n;
}

function asBool(name: string, fallback = false): boolean {
  const raw = process.env[name];
  if (!raw) return fallback;
  return raw === '1' || raw.toLowerCase() === 'true';
}

export function loadConfig(): AppConfig {
  const dataDir = process.env.EDGE_DATA_DIR ?? resolve(process.cwd(), 'data');
  return {
    port: asInt('PORT', 8080),
    host: process.env.HOST ?? '127.0.0.1',
    dbPath: process.env.EDGE_DB_PATH ?? resolve(dataDir, 'edge-commands.db'),
    leaseDurationMs: asInt('LEASE_DURATION_MS', 30_000),
    maxAttempts: asInt('MAX_ATTEMPTS', 3),
    maxAgeMs: process.env.MAX_AGE_MS ? asInt('MAX_AGE_MS', 0) : undefined,
    sweepIntervalMs: asInt('SWEEP_INTERVAL_MS', 5_000),
    claimBatchSize: asInt('CLAIM_BATCH_SIZE', 20),
    faultInjectionEnabled: asBool('FAULT_INJECTION', false),
  };
}
