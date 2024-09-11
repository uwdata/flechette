import { readFile } from 'node:fs/promises';
import { tableFromIPC as aaTable, tableToIPC as aaToIPC } from 'apache-arrow';
import { tableFromIPC as flTable, tableToIPC as flToIPC } from '../src/index.js';
import { benchmark } from './util.js';

// table encoding methods
const fl = table => flToIPC(table);
const aa = table => aaToIPC(table);

function trial(task, name, bytes, iter) {
  console.log(`${task} (${name}, ${iter} iteration${iter === 1 ? '' : 's'})`);
  const aat = aaTable(bytes);
  const flt = flTable(bytes, { useBigInt: true });
  const a = benchmark(() => aa(aat), iter);
  const f = benchmark(() => fl(flt), iter);
  const p = Object.keys(a);
  console.table(p.map(key => ({
    measure: key,
    'arrow-js': +(a[key].toFixed(2)),
    flechette: +(f[key].toFixed(2)),
    ratio: +((a[key] / f[key]).toFixed(2))
  })));
}

async function run(file, iter = 5) {
  console.log(`\n** Encoding performance using ${file} **\n`);
  const bytes = new Uint8Array(await readFile(`test/data/${file}`));
  trial('Encode Table to IPC', file, bytes, iter);
}

await run('flights.arrows');
await run('scrabble.arrows');
