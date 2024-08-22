import assert from 'node:assert';
import { arrowFromDuckDB } from './util/arrow-from-duckdb.js';
import { tableFromIPC } from '../src/index.js';
import { Table } from '../src/table.js';

const values = [
  {a: 1, b: 'foo', c: [1, null, 3] },
  null,
  {a: 2, b: 'baz', c: [null, 5, 6] }
];

const table = tableFromIPC(await arrowFromDuckDB(values));

describe('Table', () => {
  it('provides row count', () => {
    assert.deepStrictEqual(table.numRows, 3);
  });

  it('provides column count', () => {
    assert.deepStrictEqual(table.numCols, 1);
  });

  it('provides child column accessors', () => {
    const col = table.getChild('value');
    assert.strictEqual(col, table.getChildAt(0));
    assert.deepStrictEqual(col.toArray(), values);
  });

  it('provides object array', () => {
    assert.deepStrictEqual(table.toArray(), values.map(value => ({ value })));
  });

  it('provides column array map', () => {
    assert.deepStrictEqual(table.toColumns(), { value: values });
  });

  it('provides random access via at/get', () => {
    const idx = [0, 1, 2];

    // table object random access
    const obj = values.map(value => ({ value }));
    assert.deepStrictEqual(idx.map(i => table.at(i)), obj);
    assert.deepStrictEqual(idx.map(i => table.get(i)), obj);

    // column value random access
    const col = table.getChildAt(0);
    assert.deepStrictEqual(idx.map(i => col.at(i)), values);
    assert.deepStrictEqual(idx.map(i => col.get(i)), values);
  });

  it('provides select by index', async () => {
    const sel = table.selectAt([0, 0]);
    const col = table.getChild('value');
    assert.strictEqual(sel.schema.fields.length, 2);
    assert.strictEqual(sel.getChildAt(0), col);
    assert.strictEqual(sel.getChildAt(1), col);
  });

  it('provides select by index with rename', async () => {
    const sel = table.selectAt([0, 0], ['foo', 'bar']);
    const col = table.getChild('value');
    assert.strictEqual(sel.schema.fields.length, 2);
    assert.strictEqual(sel.getChildAt(0), col);
    assert.strictEqual(sel.getChildAt(1), col);
    assert.strictEqual(sel.getChild('foo'), col);
    assert.strictEqual(sel.getChild('bar'), col);
  });

  it('provides select by name', async () => {
    const sel = table.select(['value', 'value']);
    const col = table.getChild('value');
    assert.strictEqual(sel.schema.fields.length, 2);
    assert.strictEqual(sel.getChildAt(0), col);
    assert.strictEqual(sel.getChildAt(1), col);
  });

  it('provides select by name with rename', async () => {
    const sel = table.select(['value', 'value'], ['foo', 'bar']);
    const col = table.getChild('value');
    assert.strictEqual(sel.schema.fields.length, 2);
    assert.strictEqual(sel.getChildAt(0), col);
    assert.strictEqual(sel.getChildAt(1), col);
    assert.strictEqual(sel.getChild('foo'), col);
    assert.strictEqual(sel.getChild('bar'), col);
  });

  it('handles empty table with no schema', async () => {
    const test = (table) => {
      assert.strictEqual(table.numRows, 0);
      assert.strictEqual(table.numCols, 0);
      assert.deepStrictEqual(table.toColumns(), {});
      assert.deepStrictEqual(table.toArray(), []);
      assert.deepStrictEqual([...table], []);
    }
    test(new Table({ fields: [] }, []));
    test(new Table({ fields: [] }, []).select([]));
    test(new Table({ fields: [] }, []).selectAt([]));
  });

  it('handles empty table with schema', async () => {
    const fields = [
      { name: 'foo', type: { typeId: 2, bitWidth: 32, signed: true } },
      { name: 'bar', type: { typeId: 5 } }
    ];
    const test = (table) => {
      assert.strictEqual(table.numRows, 0);
      assert.strictEqual(table.numCols, 2);
      assert.deepStrictEqual(table.toColumns(), { foo: [], bar: [] });
      assert.deepStrictEqual(table.toArray(), []);
      assert.deepStrictEqual([...table], []);
    }
    test(new Table({ fields }, []));
    test(new Table({ fields }, []).select(['foo', 'bar']));
    test(new Table({ fields }, []).selectAt([0, 1]));
  });

  it('is not concat spreadable', () => {
    assert.ok(!table[Symbol.isConcatSpreadable]);
    assert.deepStrictEqual([].concat(table), [table]);
  });
});
