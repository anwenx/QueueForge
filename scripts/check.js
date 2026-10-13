// Syntax-check every JS file (no dependencies needed).
import { readdirSync, statSync } from 'node:fs';
import { join } from 'node:path';
import { spawnSync } from 'node:child_process';

const roots = ['src', 'bin', 'public', 'test', 'scripts'];
let failed = 0;
const walk = (dir) => readdirSync(dir).flatMap((f) => {
  const p = join(dir, f);
  return statSync(p).isDirectory() ? walk(p) : p.endsWith('.js') ? [p] : [];
});
for (const r of roots) {
  for (const file of walk(r)) {
    const res = spawnSync(process.execPath, ['--check', file], { encoding: 'utf8' });
    if (res.status !== 0) { failed++; console.error(`FAIL ${file}\n${res.stderr}`); }
  }
}
if (failed) { console.error(`${failed} file(s) failed syntax check`); process.exit(1); }
console.log('syntax check ok');
