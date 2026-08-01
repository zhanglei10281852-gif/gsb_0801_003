import { ApiClient } from './apiClient.js';
import { DeviceSimulator } from './deviceSimulator.js';
import { GatewaySimulator } from './gatewaySimulator.js';

interface CliArgs {
  url: string;
  gatewayId: string;
  leaseMs: number;
  pollMs: number;
  scenario: string;
}

function parseArgs(): CliArgs {
  const args = process.argv.slice(2);
  const get = (flag: string, def: string): string => {
    const idx = args.indexOf(flag);
    return idx >= 0 && idx + 1 < args.length ? args[idx + 1] : def;
  };
  return {
    url: get('--url', 'http://localhost:3000'),
    gatewayId: get('--gateway', 'gw-cli'),
    leaseMs: Number(get('--lease-ms', '10000')),
    pollMs: Number(get('--poll-ms', '200')),
    scenario: get('--scenario', 'normal'),
  };
}

async function waitForServer(api: ApiClient, timeoutMs = 10000): Promise<void> {
  const start = Date.now();
  while (Date.now() - start < timeoutMs) {
    try {
      if (await api.health()) return;
    } catch {
      await new Promise((r) => setTimeout(r, 300));
    }
  }
  throw new Error(`server not reachable`);
}

async function main(): Promise<void> {
  const args = parseArgs();
  const api = new ApiClient(args.url);
  await waitForServer(api);
  console.log(`[cli] connected to ${args.url}`);

  const device = new DeviceSimulator('dev-cli', { confirmDelayMs: 100 });
  const gw = new GatewaySimulator({
    gatewayId: args.gatewayId,
    pollIntervalMs: args.pollMs,
    leaseDurationMs: args.leaseMs,
    renewIntervalMs: Math.floor(args.leaseMs / 3),
    api,
    device,
  });

  console.log(`[cli] starting gateway "${args.gatewayId}" with scenario "${args.scenario}"`);
  gw.start();

  if (args.scenario === 'flaky') {
    setInterval(() => {
      if (gw.isOnline()) {
        console.log('[cli] simulating network disconnect');
        gw.goOffline();
        setTimeout(() => {
          console.log('[cli] network restored');
          gw.goOnline();
        }, 3000);
      }
    }, 8000);
  }

  if (args.scenario === 'dup-ack') {
    gw.setDuplicateConfirm(true);
  }

  const shutdown = async () => {
    console.log('[cli] shutting down');
    gw.stop();
    process.exit(0);
  };
  process.on('SIGINT', shutdown);
  process.on('SIGTERM', shutdown);
}

main().catch((err) => {
  console.error('[cli] fatal:', err);
  process.exit(1);
});
