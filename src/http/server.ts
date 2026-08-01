import express from 'express';
import { createRouter } from './routes.js';
import { CommandService } from '../application/commandService.js';
import { DB } from '../infrastructure/sqlite/database.js';
import { createSqliteAdapters } from '../infrastructure/sqlite/sqliteRepository.js';
import { SystemClock, UuidGenerator } from '../infrastructure/systemPrimitives.js';

export interface ServerContext {
  app: express.Express;
  service: CommandService;
  db: DB;
  stopScanner: () => void;
}

export function createServer(db: DB, opts: {
  leaseScanIntervalMs?: number;
} = {}): ServerContext {
  const { commands, events, uow } = createSqliteAdapters(db);
  const clock = new SystemClock();
  const idGen = new UuidGenerator();
  const service = new CommandService(uow, commands, events, clock, idGen);

  const app = express();
  app.use(express.json({ limit: '1mb' }));
  app.use('/api/v1', createRouter(service));

  const scanIntervalMs = opts.leaseScanIntervalMs ?? 2000;
  const scannerTimer = setInterval(() => {
    service.scanExpiredLeases(100).catch((err) => {
      console.error('[lease-scanner] error:', err);
    });
  }, scanIntervalMs);
  scannerTimer.unref();

  const stopScanner = () => clearInterval(scannerTimer);

  return { app, service, db, stopScanner };
}

export function startServer(db: DB, port: number, opts: {
  leaseScanIntervalMs?: number;
} = {}): Promise<{ context: ServerContext; close: () => Promise<void> }> {
  return new Promise((resolve) => {
    const context = createServer(db, opts);
    const server = context.app.listen(port, () => {
      console.log(`[server] listening on http://localhost:${port}`);
      const close = () =>
        new Promise<void>((res) => {
          context.stopScanner();
          server.close(() => res());
        });
      resolve({ context, close });
    });
  });
}
