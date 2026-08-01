/**
 * 服务入口:装配 SQLite 适配器、应用服务、HTTP 适配器与租约巡检器。
 */
import { loadConfig } from './config';
import { DispatchService } from './service/dispatchService';
import { SqliteRepository } from './storage/sqlite';
import { createServer } from './http/server';

const config = loadConfig();
const repo = new SqliteRepository(config.dbPath);
const service = new DispatchService(repo, config);
const server = createServer(service, config);

// 巡检器:周期性结算过期租约,让失联网关的任务被重新派发或终结。
// 语义不依赖巡检间隔——领取/续约/确认路径会按同一时钟判定惰性结算。
const reaper = setInterval(() => {
  try {
    service.expireDue('reaper');
  } catch (err) {
    console.error('[reaper]', err);
  }
}, config.reaperIntervalMs);

server.listen(config.port, () => {
  console.log(
    `[dispatch] listening on :${config.port} db=${config.dbPath} ` +
      `leaseTtl=${config.leaseTtlMs}ms maxAttempts=${config.maxAttempts}`,
  );
});

function shutdown(): void {
  clearInterval(reaper);
  server.close(() => {
    repo.close();
    process.exit(0);
  });
  setTimeout(() => process.exit(1), 3000).unref();
}

process.on('SIGINT', shutdown);
process.on('SIGTERM', shutdown);
