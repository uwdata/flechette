import { describe, it, expect } from "vitest";
import { float64, int8, int32, bool, dictionary, tableFromArrays, utf8, float32, nullType } from '../src/index.js';

describe('tableFromArrays', () => {
  const values = {
    foo: [1, 2, 3, 4, 5],
    bar: [1.3, NaN, 1e27, Math.PI, Math.E].map(v => Math.fround(v)),
    baz: [true, false, null, false, true],
    bop: ['foo', 'bar', 'baz', 'bop', 'bip']
  };

  const types = {
    foo: int8(),
    bar: float64(),
    baz: bool(),
    bop: dictionary(utf8(), int32())
  };

  function check(table, colTypes = types) {
    const { fields } = table.schema;
    expect(table.numRows).toBe(5);
    expect(table.numCols).toBe(4);
    table.children.forEach((c, i) => {
      const { name } = fields[i];
      expect(c.type).toStrictEqual(colTypes[name]);
      expect(fields[i].type).toStrictEqual(colTypes[name]);
      expect([...c]).toStrictEqual(values[name]);
    });
    return table;
  }

  it('creates table from provided types', () => {
    // with types that match type inference results
    check(tableFromArrays(values, { types }));
    check(tableFromArrays(Object.entries(values), { types }));

    // with types that do not match type inference reults
    const opt = { types: { ...types, foo: int32(), bar: float32() } };
    check(tableFromArrays(values, opt), opt.types);
    check(tableFromArrays({
      ...values,
      foo: Int16Array.from(values.foo),
      bar: Float64Array.from(values.bar)
    }, opt), opt.types);
  });

  it('creates table from inferred types', () => {
    check(tableFromArrays(values));
    check(tableFromArrays(Object.entries(values)));

    // infer from typed arrays
    check(tableFromArrays({
      ...values,
      foo: Int8Array.from(values.foo),
      bar: Float64Array.from(values.bar)
    }));
  });

  it('creates empty table', () => {
    const withoutCols = tableFromArrays({});
    expect(withoutCols.numRows).toBe(0);
    expect(withoutCols.numCols).toBe(0);
    expect(withoutCols.schema.fields).toStrictEqual([]);

    const withCols = tableFromArrays({ foo: [], bar: [] });
    expect(withCols.numRows).toBe(0);
    expect(withCols.numCols).toBe(2);
    expect(withCols.schema.fields.map(f => f.type))
      .toStrictEqual([ nullType(), nullType() ]);

    const withTypes = tableFromArrays(
      { foo: [], bar: [] },
      { types: { foo: int32(), bar: float32() }}
    );
    expect(withTypes.numRows).toBe(0);
    expect(withTypes.numCols).toBe(2);
    expect(withTypes.schema.fields.map(f => f.type))
      .toStrictEqual([ int32(), float32() ]);
  });

  it('throws when array lengths differ', () => {
    expect(() => tableFromArrays({ foo: [1, 2, 3], bar: [1, 2] })).throws();
  });
});
