import { readFile } from 'node:fs/promises';
import { tableFromIPC as aaTable } from 'apache-arrow';
import { tableFromIPC as flTable } from '../src/index.js';
import { benchmark } from './util.js';

// table creation
const fl = bytes => flTable(bytes, { useBigInt: true, useProxy: true });
const aa = bytes => aaTable(bytes);

// decode ipc data to columns
function decodeIPC(table) {
  return table.schema.fields.map((f, i) => table.getChildAt(i));
}

// extract column arrays directly
function extractArrays(table) {
  const n = table.numCols;
  const data = [];
  for (let i = 0; i < n; ++i) {
    data.push(table.getChildAt(i).toArray());
  }
  return data;
}

// iterate over values for each column
function iterateValues(table) {
  const names = table.schema.fields.map(f => f.name);
  names.forEach(name => Array.from(table.getChild(name)));
}

// random access to each column value
// this will be slower if there are multiple record batches
// due to the need for binary search over the offsets array
function randomAccess(table) {
  const { numRows, numCols } = table;
  const vals = Array(numCols);
  for (let j = 0; j < numCols; ++j) {
    const col = table.getChildAt(j);
    for (let i = 0; i < numRows; ++i) {
      vals[j] = col.at(i);
    }
  }
}

// generate row objects, access each property
function visitObjects(table) {
  const nr = table.numRows;
  const names = table.schema.fields.map(f => f.name);
  const obj = table.toArray();
  for (let i = 0; i < nr; ++i) {
    const row = obj[i];
    names.forEach(name => row[name]);
  }
}

function trial(task, name, bytes, method, iter) {
  console.log(`${task} (${name}, ${iter} iteration${iter === 1 ? '' : 's'})`);
  const a = benchmark(() => method(aa(bytes)), iter);
  const f = benchmark(() => method(fl(bytes)), iter);
  const p = Object.keys(a);
  console.table(p.map(key => ({
    measure: key,
    'arrow-js': +(a[key].toFixed(2)),
    flechette: +(f[key].toFixed(2)),
    ratio: +((a[key] / f[key]).toFixed(2))
  })));
}

async function run(file, iter = 5) {
  console.log(`\n** Decoding performance using ${file} **\n`);
  const bytes = new Uint8Array(await readFile(`test/data/${file}`));
  trial('Decode Table from IPC', file, bytes, decodeIPC, iter);
  trial('Extract Arrays', file, bytes, extractArrays, iter);
  trial('Iterate Values', file, bytes, iterateValues, iter);
  trial('Random Access', file, bytes, randomAccess, iter);
  trial('Visit Row Objects', file, bytes, visitObjects, iter);
}

await run('flights.arrows');
await run('scrabble.arrows');
