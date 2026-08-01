import { ApiClient } from './api-client.js';
import { GatewaySimulator } from './gateway.js';
import { builtInScenarios, customDevice } from './scenarios.js';

interface CliOptions {
  url: string;
  command?: string;
  scenario?: string;
  gatewayId?: string;
  deviceId?: string;
  payloadType?: string;
  key?: string;
  leaseMs?: number;
  iterations?: number;
  delayMs?: number;
  device?: 'ack' | 'nack' | 'slow';
}

function parseArgs(argv: string[]): CliOptions {
  const opts: CliOptions = {
    url: process.env.EDGE_URL ?? 'http://127.0.0.1:8080',
    device: 'ack',
  };
  for (let i = 0; i < argv.length; i++) {
    const arg = argv[i];
    switch (arg) {
      case '--url': opts.url = argv[++i]; break;
      case 'scenario': opts.command = 'scenario'; opts.scenario = argv[++i]; break;
      case 'submit': opts.command = 'submit'; break;
      case 'poll': opts.command = 'poll'; break;
      case 'loop': opts.command = 'loop'; break;
      case 'query': opts.command = 'query'; break;
      case 'events': opts.command = 'events'; break;
      case '--gateway': opts.gatewayId = argv[++i]; break;
      case '--device': opts.deviceId = argv[++i]; break;
      case '--type': opts.payloadType = argv[++i]; break;
      case '--key': opts.key = argv[++i]; break;
      case '--lease-ms': opts.leaseMs = Number(argv[++i]); break;
      case '--iterations': opts.iterations = Number(argv[++i]); break;
      case '--delay-ms': opts.delayMs = Number(argv[++i]); break;
      case '--behavior': opts.device = argv[++i] as CliOptions['device']; break;
      default:
        if (!arg.startsWith('--') && !opts.command) opts.command = arg;
    }
  }
  return opts;
}

async function runScenario(client: ApiClient, name: string) {
  const scenario = builtInScenarios[name] ?? (name === 'all' ? undefined : null);
  if (scenario === null) {
    console.error(`unknown scenario: ${name}`);
    console.error(`available: all, ${Object.keys(builtInScenarios).join(', ')}`);
    process.exit(2);
  }
  const names = typeof scenario === 'function' ? [name] : Object.keys(builtInScenarios);
  let failed = 0;
  for (const n of names) {
    process.stdout.write(`scenario ${n} ... `);
    const result = await builtInScenarios[n](client);
    console.log(result.passed ? 'PASS' : 'FAIL');
    for (const d of result.details) console.log(`  - ${d}`);
    if (result.data) console.log(`  data=${JSON.stringify(result.data)}`);
    if (!result.passed) failed++;
  }
  if (failed > 0) process.exit(1);
}

async function runSubmit(client: ApiClient, opts: CliOptions) {
  const key = opts.key ?? `cli-${Date.now()}`;
  const res = await client.submit({
    idempotencyKey: key,
    deviceId: opts.deviceId ?? 'device-cli',
    payload: { type: opts.payloadType ?? 'CALIBRATE' },
  });
  console.log(JSON.stringify({ status: res.status, body: res.body }, null, 2));
}

async function runPoll(client: ApiClient, opts: CliOptions) {
  const gw = new GatewaySimulator(
    opts.gatewayId ?? 'gw-cli',
    client,
    customDevice(opts.device ?? 'ack')
  );
  const leased = await gw.pollOnce(opts.leaseMs);
  if (!leased) {
    console.log(JSON.stringify({ claimed: false }, null, 2));
    return;
  }
  const ack = await gw.ack(leased);
  console.log(JSON.stringify({ claimed: true, leased, ackStatus: ack.status, ackBody: ack.body }, null, 2));
}

async function runLoop(client: ApiClient, opts: CliOptions) {
  const gw = new GatewaySimulator(
    opts.gatewayId ?? 'gw-loop',
    client,
    customDevice(opts.device ?? 'ack')
  );
  const iterations = opts.iterations ?? 5;
  const delayMs = opts.delayMs ?? 500;
  for (let i = 0; i < iterations; i++) {
    const leased = await gw.pollOnce(opts.leaseMs ?? 10_000);
    if (leased) {
      const ack = await gw.ack(leased);
      console.log(`iter=${i} command=${leased.commandId} attempt=${leased.attempt} ack=${ack.status}`);
    } else {
      console.log(`iter=${i} no claimable command`);
    }
    await new Promise((r) => setTimeout(r, delayMs));
  }
}

async function runQuery(client: ApiClient, opts: CliOptions) {
  if (!opts.key) throw new Error('--key required for query');
  const res = await client.getByIdempotencyKey(opts.key);
  console.log(JSON.stringify({ status: res.status, body: res.body }, null, 2));
}

async function runEvents(client: ApiClient) {
  const res = await client.adminEvents(500);
  console.log(JSON.stringify(res.body, null, 2));
}

async function main() {
  const opts = parseArgs(process.argv.slice(2));
  const client = new ApiClient(opts.url);
  await GatewaySimulator.waitForHealth(client);
  switch (opts.command) {
    case 'scenario': await runScenario(client, opts.scenario ?? 'all'); break;
    case 'submit': await runSubmit(client, opts); break;
    case 'poll': await runPoll(client, opts); break;
    case 'loop': await runLoop(client, opts); break;
    case 'query': await runQuery(client, opts); break;
    case 'events': await runEvents(client); break;
    default:
      console.log(`
edge-command simulator CLI

usage:
  npm run simulate -- scenario all
  npm run simulate -- scenario lease-retry
  npm run simulate -- submit --key order-123 --device line-1 --type CALIBRATE
  npm run simulate -- poll --gateway gw-1 --behavior ack
  npm run simulate -- loop --gateway gw-1 --iterations 10 --delay-ms 1000
  npm run simulate -- query --key order-123
  npm run simulate -- events
`);
  }
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
