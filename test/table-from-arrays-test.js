import assert from 'node:assert';
import { float64, int8, int32, bool, dictionary, tableFromArrays, utf8 } from '../src/index.js';

describe('tableFromArrays', () => {
  const values = {
    foo: [1, 2, 3, 4, 5],
    bar: [true, false, null, false, true],
    baz: [1.3, NaN, 1e27, Math.PI, Math.E],
    bop: ['foo', 'bar', 'baz', 'bop', 'bip']
  };

  const types = [
    int8(),
    bool(),
    float64(),
    dictionary(utf8(), int32())
  ];

  function check(table) {
    const { fields } = table.schema;
    assert.strictEqual(table.numRows, 5);
    assert.strictEqual(table.numCols, 4);
    table.children.forEach((c, i) => {
      assert.deepStrictEqual(c.type, types[i]);
      assert.deepStrictEqual(fields[i].type, types[i]);
      assert.deepStrictEqual([...c], values[fields[i].name]);
    });
    return table;
  }

  it('creates table from provided types', () => {
    const typeMap = {
      foo: types[0],
      bar: types[1],
      baz: types[2],
      bop: types[3]
    };
    check(tableFromArrays(values, typeMap));
    check(tableFromArrays(Object.entries(values), typeMap));
  });

  it('creates table from inferred types', () => {
    check(tableFromArrays(values));
    check(tableFromArrays(Object.entries(values)));

    // infer from typed arrays
    check(tableFromArrays({
      ...values,
      foo: Int8Array.from(values.foo),
      baz: Float64Array.from(values.baz)
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
