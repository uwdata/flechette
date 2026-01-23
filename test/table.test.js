/** @import { ValueArray } from '../src/types.js' */
import { describe, it, expect } from "vitest";
import { readFile } from 'node:fs/promises';
import { tableFromIPC } from '../src/index.js';
import { Table } from '../src/table.js';

describe('Table', async () => {
  const values = [
    {a: 1, b: 'foo', c: [1, null, 3] },
    null,
    {a: 2, b: 'baz', c: [null, 5, 6] }
  ];

  const bytes = new Uint8Array(await readFile(`test/data/table.arrows`));

  /** @type {Table<{ value: { a: number, b: string, c: ValueArray<number | null> }> }} */
  const table = tableFromIPC(bytes);

  it('provides row count', () => {
    expect(table.numRows).toBe(3);
  });

  it('provides column count', () => {
    expect(table.numCols).toBe(1);
  });

  it('provides child column accessors', () => {
    const col = table.getChild('value');
    expect(col).toBe(table.getChildAt(0));
    expect(col.toArray()).toStrictEqual(values);
  });

  it('provides object array', () => {
    expect(table.toArray()).toStrictEqual(values.map(value => ({ value })));
  });

  it('provides column array map', () => {
    expect(table.toColumns()).toStrictEqual({ value: values });
  });

  it('provides random access via at/get', () => {
    const idx = [0, 1, 2];

    // table object random access
    const obj = values.map(value => ({ value }));
    expect(idx.map(i => table.at(i))).toStrictEqual(obj);
    expect(idx.map(i => table.get(i))).toStrictEqual(obj);

    // column value random access
    const col = table.getChildAt(0);
    expect(idx.map(i => col.at(i))).toStrictEqual(values);
    expect(idx.map(i => col.get(i))).toStrictEqual(values);
  });

  it('provides select by index', async () => {
    const sel = table.selectAt([0, 0]);
    const col = table.getChild('value');
    expect(sel.schema.fields.length).toBe(2);
    expect(sel.getChildAt(0)).toBe(col);
    expect(sel.getChildAt(1)).toBe(col);
  });

  it('provides select by index with rename', async () => {
    const sel = table.selectAt([0, 0], ['foo', 'bar']);
    const col = table.getChild('value');
    expect(sel.schema.fields.length).toBe(2);
    expect(sel.getChildAt(0)).toBe(col);
    expect(sel.getChildAt(1)).toBe(col);
    expect(sel.getChild('foo')).toBe(col);
    expect(sel.getChild('bar')).toBe(col);
  });

  it('provides select by name', async () => {
    const sel = table.select(['value', 'value']);
    const col = table.getChild('value');
    expect(sel.schema.fields.length).toBe(2);
    expect(sel.getChildAt(0)).toBe(col);
    expect(sel.getChildAt(1)).toBe(col);
  });

  it('provides select by name with rename', async () => {
    const sel = table.select(['value', 'value'], ['foo', 'bar']);
    const col = table.getChild('value');
    expect(sel.schema.fields.length).toBe(2);
    expect(sel.getChildAt(0)).toBe(col);
    expect(sel.getChildAt(1)).toBe(col);
    expect(sel.getChild('foo')).toBe(col);
    expect(sel.getChild('bar')).toBe(col);
  });

  it('handles empty table with no schema', async () => {
    const test = (table) => {
      expect(table.numRows).toBe(0);
      expect(table.numCols).toBe(0);
      expect(table.toColumns()).toStrictEqual({});
      expect(table.toArray()).toStrictEqual([]);
      expect([...table]).toStrictEqual([]);
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
      expect(table.numRows).toBe(0);
      expect(table.numCols).toBe(2);
      expect(table.toColumns()).toStrictEqual({ foo: [], bar: [] });
      expect(table.toArray()).toStrictEqual([]);
      expect([...table]).toStrictEqual([]);
    }
    test(new Table({ fields }, []));
    test(new Table({ fields }, []).select(['foo', 'bar']));
    test(new Table({ fields }, []).selectAt([0, 1]));
  });

  it('is not concat spreadable', () => {
    expect(table[Symbol.isConcatSpreadable]).toBeFalsy();
    expect([].concat(table)).toStrictEqual([table]);
  });
});
