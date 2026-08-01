import path from 'node:path';
import { fileURLToPath } from 'node:url';
import { openDatabase, closeDatabase } from './infrastructure/sqlite/database.js';
import { startServer } from './http/server.js';

const __filename = fileURLToPath(import.meta.url);
const __dirname = path.dirname(__filename);

async function main() {
  const port = Number(process.env.PORT) || 3000;
  const dbPath =
    process.env.DB_PATH || path.join(__dirname, '..', 'data', 'commands.db');

  console.log(`[boot] opening database at ${dbPath}`);
  const db = openDatabase(dbPath);

  const { context, close } = await startServer(db, port);

  const shutdown = async (signal: string) => {
    console.log(`[shutdown] received ${signal}, closing...`);
    await close();
    closeDatabase(db);
    console.log('[shutdown] done');
    process.exit(0);
  };

  process.on('SIGINT', () => shutdown('SIGINT'));
  process.on('SIGTERM', () => shutdown('SIGTERM'));

  console.log('[boot] ready');
  void context;
}

main().catch((err) => {
  console.error('[fatal]', err);
  process.exit(1);
});
