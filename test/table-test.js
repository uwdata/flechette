import assert from 'node:assert';
import { arrowFromDuckDB } from './util/arrow-from-duckdb.js';
import { tableFromIPC } from '../src/index.js';

const values = [
  {a: 1, b: 'foo', c: [1, null, 3] },
  null,
  {a: 2, b: 'baz', c: [null, 5, 6] }
];

const table = tableFromIPC(await arrowFromDuckDB(values));

describe('Table', () => {
  it('provides row count', async () => {
    assert.deepStrictEqual(table.numRows, 3);
  });

  it('provides column count', async () => {
    assert.deepStrictEqual(table.numCols, 1);
  });

  it('provides child column accessors', async () => {
    const col = table.getChild('value');
    assert.strictEqual(col, table.getChildAt(0));
    assert.deepStrictEqual(col.toArray(), values);
  });

  it('provides object array', async () => {
    assert.deepStrictEqual(table.toArray(), values.map(value => ({ value })));
  });

  it('provides column array map', async () => {
    assert.deepStrictEqual(table.toColumns(), { value: values });
  });

  it('provides select by index', async () => {
    const sel = table.selectAt([0, 0]);
    const col = table.getChild('value');
    assert.strictEqual(sel.schema.fields.length, 2);
    assert.strictEqual(sel.getChildAt(0), col);
    assert.strictEqual(sel.getChildAt(1), col);
  });

  it('provides select by name', async () => {
    const sel = table.select(['value', 'value']);
    const col = table.getChild('value');
    assert.strictEqual(sel.schema.fields.length, 2);
    assert.strictEqual(sel.getChildAt(0), col);
    assert.strictEqual(sel.getChildAt(1), col);
  });
});
