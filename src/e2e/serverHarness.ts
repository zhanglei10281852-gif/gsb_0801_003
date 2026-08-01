import { spawn, ChildProcess } from "node:child_process";
import path from "node:path";
import fs from "node:fs";
import os from "node:os";
import { fileURLToPath } from "node:url";

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

export interface ServerHandle {
  port: number;
  baseUrl: string;
  dbPath: string;
  stop: () => Promise<void>;
}

export interface ManagedServer {
  port: number;
  baseUrl: string;
  dbPath: string;
  start: () => Promise<void>;
  stop: () => Promise<void>;
  restart: () => Promise<void>;
  killAbruptly: () => Promise<void>;
}

function distEntry(): string {
  const entry = path.join(__dirname, "..", "index.js");
  if (!fs.existsSync(entry)) {
    throw new Error(
      `Compiled server not found at ${entry}. Run "npm run build" first.`,
    );
  }
  return entry;
}

export async function startCompiledServer(
  port?: number,
): Promise<ServerHandle> {
  const chosenPort = port ?? 3000 + Math.floor(Math.random() * 1000);
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "edge-cmd-e2e-"));
  const dbPath = path.join(tmpDir, "commands.db");
  const child = await spawnAndWait(chosenPort, dbPath);

  const stop = () => killChild(child).then(() => cleanupDir(tmpDir));

  return {
    port: chosenPort,
    baseUrl: `http://localhost:${chosenPort}`,
    dbPath,
    stop,
  };
}

export function createManagedServer(port?: number): ManagedServer {
  const chosenPort = port ?? 3000 + Math.floor(Math.random() * 1000);
  const tmpDir = fs.mkdtempSync(path.join(os.tmpdir(), "edge-cmd-e2e-"));
  const dbPath = path.join(tmpDir, "commands.db");
  let child: ChildProcess | null = null;

  async function spawnProcess(): Promise<void> {
    child = await spawnAndWait(chosenPort, dbPath);
  }

  async function terminate(signal: NodeJS.Signals): Promise<void> {
    if (!child) return;
    const proc = child;
    child = null;
    await killChild(proc, signal);
  }

  return {
    port: chosenPort,
    baseUrl: `http://localhost:${chosenPort}`,
    dbPath,
    async start() {
      await spawnProcess();
    },
    async stop() {
      await terminate("SIGTERM");
      cleanupDir(tmpDir);
    },
    async restart() {
      await terminate("SIGTERM");
      await spawnProcess();
    },
    async killAbruptly() {
      await terminate("SIGKILL");
    },
  };
}

function cleanupDir(dir: string): void {
  try {
    fs.rmSync(dir, { recursive: true, force: true });
  } catch {
    // ignore
  }
}

function killChild(
  child: ChildProcess,
  signal: NodeJS.Signals = "SIGTERM",
): Promise<void> {
  return new Promise((resolve) => {
    const timer = setTimeout(() => {
      child.kill("SIGKILL");
      resolve();
    }, 5000);
    child.on("exit", () => {
      clearTimeout(timer);
      resolve();
    });
    child.kill(signal);
  });
}

async function spawnAndWait(
  port: number,
  dbPath: string,
): Promise<ChildProcess> {
  const child: ChildProcess = spawn(process.execPath, [distEntry()], {
    env: {
      ...process.env,
      PORT: String(port),
      DB_PATH: dbPath,
    },
    stdio: ["ignore", "pipe", "pipe"],
  });

  const baseUrl = `http://localhost:${port}`;
  await waitForHealth(`${baseUrl}/api/v1/health`, 15000);
  return child;
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
  throw new Error(
    `Server did not become healthy at ${url} within ${timeoutMs}ms`,
  );
}

export function sleep(ms: number): Promise<void> {
  return new Promise((r) => setTimeout(r, ms));
}
