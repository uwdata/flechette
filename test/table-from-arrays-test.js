import assert from 'node:assert';
import { float64, int8, int32, bool, dictionary, tableFromArrays, utf8, float32 } from '../src/index.js';

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
    assert.strictEqual(table.numRows, 5);
    assert.strictEqual(table.numCols, 4);
    table.children.forEach((c, i) => {
      const { name } = fields[i];
      assert.deepStrictEqual(c.type, colTypes[name]);
      assert.deepStrictEqual(fields[i].type, colTypes[name]);
      assert.deepStrictEqual([...c], values[name]);
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
    const table = tableFromArrays({});
    assert.strictEqual(table.numRows, 0);
    assert.strictEqual(table.numCols, 0);
  });

  it('throws when arrays lengths differ', () => {
    assert.throws(() => tableFromArrays({ foo: [1, 2, 3], bar: [1, 2] }));
  });
});
