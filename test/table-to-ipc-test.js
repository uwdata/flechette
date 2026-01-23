import assert from 'node:assert';
import { readFile } from 'node:fs/promises';
import { Version, columnFromArray, tableFromColumns, tableFromIPC, tableToIPC } from '../src/index.js';
import * as dataMethods from './util/data.js';

const files = [
  'flights.arrows',
  'scrabble.arrows',
  'convert.arrows',
  'decimal_test.arrows'
];

describe('tableToIPC', () => {
  for (const [name, method] of Object.entries(dataMethods)) {
    it(`encodes ${name} data`, async () => {
      const data = await method();
      data.forEach(({ bytes }) => testEncode(bytes, 'stream'));
      data.forEach(({ bytes }) => testEncode(bytes, 'file'));
    });
  }

  for (const file of files) {
    it(`encodes ${file}`, async () => {
      const bytes = new Uint8Array(await readFile(`test/data/${file}`));
      testEncode(bytes, 'stream');
      testEncode(bytes, 'file');
    });
  }

  it('throws on inconsistent batch sizes', () => {
    const a = columnFromArray([1, 2, 3, 4, 5], null);
    const b = columnFromArray([1, 2, 3, 4, 5], null, { maxBatchRows: 2 });
    assert.throws(() => tableToIPC(tableFromColumns({ a, b })));
    assert.throws(() => tableToIPC(tableFromColumns({ b, a })));
  });
});

function testEncode(bytes, format = 'stream') {
  // load table
  const table = tableFromIPC(bytes);

  // ensure complete schema, override version
  const schema = {
    endianness: 0,
    metadata: null,
    ...table.schema,
    version: Version.V5
  };

  // encode table to ipc bytes
  const ipc = tableToIPC(table, { format });

  // parse ipc bytes to get a "round-trip" table
  const round = tableFromIPC(ipc);

  // check schema and shape equality
  assert.deepStrictEqual(round.schema, schema);
  assert.strictEqual(round.numRows, table.numRows);
  assert.strictEqual(round.numCols, table.numCols);

  // check extracted value equality
  for (let i = 0; i < table.numCols; ++i) {
    assert.deepStrictEqual(
      round.getChildAt(i).toArray(),
      table.getChildAt(i).toArray()
    );
  }
}
