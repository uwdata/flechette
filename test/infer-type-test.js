import assert from 'node:assert';
import { bool, dateDay, dictionary, float64, int16, int32, int64, int8, list, struct, timestamp, utf8 } from '../src/index.js';
import { inferType } from '../src/build/infer-type.js';

function matches(actual, expect) {
  assert.deepStrictEqual(actual, expect);
}

describe('inferType', () => {
  it('infers integer types', () => {
    matches(inferType([1, 2, 3]), int8());
    matches(inferType([1e3, 2e3, 3e3]), int16());
    matches(inferType([1e6, 2e6, 3e6]), int32());
    matches(inferType([1n, 2n, 3n]), int64());

    matches(inferType([-1, 2, 3]), int8());
    matches(inferType([-1e3, 2e3, 3e3]), int16());
    matches(inferType([-1e6, 2e6, 3e6]), int32());
    matches(inferType([-1n, 2n, 3n]), int64());

    matches(inferType([1, 2, null, undefined, 3]), int8());
    matches(inferType([1e3, 2e3, null, undefined, 3e3]), int16());
    matches(inferType([1e6, 2e6, null, undefined, 3e6]), int32());
    matches(inferType([1n, 2n, null, undefined, 3n]), int64());
  });

  it('infers float types', () => {
    matches(inferType([1.1, 2.2, 3.3]), float64());
    matches(inferType([-1.1, 2.2, 3.3]), float64());
    matches(inferType([1, 2, 3.3]), float64());
    matches(inferType([1, 2, NaN]), float64());
    matches(inferType([NaN, null, undefined, NaN]), float64());
    matches(inferType([Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]), float64());
  });

  it('infers utf8 dictionary types', () => {
    const type = dictionary(utf8(), int32());
    matches(inferType(['foo', 'bar', 'baz']), type);
    matches(inferType(['foo', 'bar', null, undefined, 'baz']), type);
  });

  it('infers bool types', () => {
    matches(inferType([true, false, true]), bool());
    matches(inferType([true, false, null, undefined, true]), bool());
  });

  it('infers date day types', () => {
    matches(inferType([
      new Date(Date.UTC(2000, 1, 2)),
      new Date(Date.UTC(2006, 3, 20)),
      null,
      undefined
    ]), dateDay());
  });

  it('infers timestamp types', () => {
    matches(
      inferType([
        new Date(Date.UTC(2000, 1, 2)),
        new Date(Date.UTC(2006, 3, 20)),
        null,
        undefined,
        new Date(1990, 3, 12, 5, 37)
      ]),
      timestamp()
    );
  });

  it('infers list types', () => {
    matches(inferType([[1, 2], [3, 4]]), list(int8()));
    matches(inferType([[true, null, false], null, undefined, [false, undefined, true]]), list(bool()));
    matches(inferType([['foo', 'bar', null], null, ['bar', 'baz']]), list(dictionary(utf8(), int32())));
  });

  it('infers struct types', () => {
    matches(
      inferType([
        { foo: 1, bar: [1.1, 2.2] },
        { foo: null, bar: [2.2, null, 3.3] },
        null,
        undefined,
        { foo: 2, bar: null },
      ]),
      struct({ foo: int8(), bar: list(float64()) })
    );
  });

  it('throws on bigints that exceed 64 bits', () => {
    assert.throws(() => inferType([(1n << 200n)]));
  });

  it('throws on mixed types', () => {
    assert.throws(() => inferType([1, true, 'foo']));
  });
});
