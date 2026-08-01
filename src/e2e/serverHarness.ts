import { spawn, ChildProcess } from 'node:child_process';
import path from 'node:path';
import fs from 'node:fs';
import os from 'node:os';
import { fileURLToPath } from 'node:url';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export interface ServerHandle {
  port: number;
  baseUrl: string;
  dbPath: string;
  stop: () => Promise<void>;
}

export async function startCompiledServer(port?: number): Promise<ServerHandle> {
  const chosenPort = port ?? 3000 + Math.floor(Math.random() * 1000);
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), 'edge-cmd-e2e-'));
  const dbPath = path.join(tmpDir, 'commands.db');
  const distEntry = path.join(__dirname, '..', 'index.js');

  if (!fs.existsSync(distEntry)) {
    throw new Error(
      `Compiled server not found at ${distEntry}. Run "npm run build" first.`
    );
  }

  const child: ChildProcess = spawn(process.execPath, [distEntry], {
    env: {
      ...process.env,
      PORT: String(chosenPort),
      DB_PATH: dbPath,
    },
    stdio: ['ignore', 'pipe', 'pipe'],
  });

  let stdout = '';
  let stderr = '';
  child.stdout?.on('data', (d: Buffer) => {
    stdout += d.toString();
  });
  child.stderr?.on('data', (d: Buffer) => {
    stderr += d.toString();
  });

  const baseUrl = `http://localhost:${chosenPort}`;
  const healthUrl = `${baseUrl}/api/v1/health`;

  await waitForHealth(healthUrl, 15000);

  const stop = () =>
    new Promise<void>((resolve) => {
      const timer = setTimeout(() => {
        child.kill('SIGKILL');
        cleanup();
        resolve();
      }, 5000);
      child.on('exit', () => {
        clearTimeout(timer);
        cleanup();
        resolve();
      });
      child.kill('SIGTERM');
    });

  function cleanup() {
    try {
      fs.rmSync(tmpDir, { recursive: true, force: true });
    } catch {
      // ignore
    }
  }

  return {
    port: chosenPort,
    baseUrl,
    dbPath,
    stop,
  };
}

async function waitForHealth(url: string, timeoutMs: number): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      const res = await fetch(url);
      if (res.ok) return;
    } catch {
      await new Promise((r) => setTimeout(r, 200));
    }
  }
  throw new Error(`Server did not become healthy at ${url} within ${timeoutMs}ms`);
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
