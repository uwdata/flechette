import assert from 'node:assert';
import { readFile } from 'node:fs/promises';
import { Version, tableFromIPC, tableToIPC } from '../src/index.js';
import * as dataMethods from './util/data.js';

const files = [
  'flights.arrows',
  'scrabble.arrows',
  'convert.arrows',
  'decimal.arrows'
];

describe('tableToIPC', () => {
  for (const [name, method] of Object.entries(dataMethods)) {
    it(`encodes ${name} data`, async () => {
      const data = await method();
      data.forEach(({ bytes }) => testEncode(bytes));
    });
  }

  for (const file of files) {
    it(`encodes ${file}`, async () => {
      const bytes = new Uint8Array(await readFile(`test/data/${file}`));
      testEncode(bytes);
    });
  }
});

function testEncode(bytes) {
  // load table
  const table = tableFromIPC(bytes);

  // ensure complete schema, override version
  const schema = {
    dictionaryTypes: new Map,
    endianness: 0,
    metadata: null,
    ...table.schema,
    version: Version.V5
  };

  // encode table to ipc bytes
  const ipc = tableToIPC(table);

  // parse ipc byte to get a "round-trip" table
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
