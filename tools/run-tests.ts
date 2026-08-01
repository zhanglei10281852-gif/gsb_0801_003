import { spawn } from 'node:child_process';
import { existsSync, readdirSync, statSync } from 'node:fs';
import { join, resolve } from 'node:path';

function walk(dir: string, acc: string[] = []): string[] {
  if (!existsSync(dir)) return acc;
  for (const entry of readdirSync(dir)) {
    const full = join(dir, entry);
    const st = statSync(full);
    if (st.isDirectory()) {
      walk(full, acc);
    } else if (full.endsWith('.test.ts')) {
      acc.push(resolve(full));
    }
  }
  return acc;
}

async function main() {
  const files = [...walk('src'), ...walk('test')].sort();
  if (files.length === 0) {
    console.error('no test files found');
    process.exit(1);
  }
  const tsxBin = existsSync(resolve('node_modules/tsx/dist/cli.mjs'))
    ? resolve('node_modules/tsx/dist/cli.mjs')
    : 'tsx';
  const child = spawn(process.execPath, [tsxBin, '--test', ...files], {
    stdio: 'inherit',
    shell: false,
  });
  child.on('exit', (code) => process.exit(code ?? 1));
}

main().catch((err) => {
  console.error(err);
  process.exit(1);
});
