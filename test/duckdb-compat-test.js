import assert from 'node:assert';
import { DuckDB } from '@uwdata/mosaic-duckdb';
import { tableFromArrays, tableFromIPC, tableToIPC } from '../src/index.js';
import * as dataMethods from './util/data.js';

// Arrow types not supported by DuckDB
const skip = new Set([
  'binaryView', 'empty', 'largeListView', 'listView',
  'runEndEncoded32', 'runEndEncoded64', 'utf8View',
  'decimal32', 'decimal64', 'decimal128', 'decimal256'
]);

describe('DuckDB compatibility', () => {
  for (const [name, method] of Object.entries(dataMethods)) {
    if (skip.has(name)) continue;
    it(`includes ${name} data`, async () => {
      const data = await method();
      const load = await Promise.all(
        data.map(({ bytes }) => loadIPC(tableFromIPC(bytes)))
      );
      assert.deepStrictEqual(load, Array(data.length).fill(true));
    });
  }

  it('includes default dictionary types', async () => {
    const t = tableFromArrays({ foo: ['x', 'y', 'z'] });
    assert.strictEqual(await loadIPC(t), true);
  });
});

function loadIPC(table) {
  const bytes = tableToIPC(table, { format: 'stream' });
  return new Promise((resolve) => {
    const db = new DuckDB();
    db.db.register_buffer('arrow_ipc', [bytes], true, (err) => {
      if (err) {
        console.error(err);
        resolve(false);
      } else {
        resolve(true);
      }
    });
  });
}
