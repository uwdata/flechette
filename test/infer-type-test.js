import assert from 'node:assert';
import { bool, dateDay, dictionary, fixedSizeList, float64, int16, int32, int64, int8, list, struct, timestamp, utf8 } from '../src/index.js';
import { inferType } from '../src/build/infer-type.js';

function matches(actual, expect) {
  assert.deepStrictEqual(actual, expect);
}

function infer(values) {
  return inferType(visitor => values.forEach(visitor));
}

describe('inferType', () => {
  it('infers integer types', () => {
    matches(infer([1, 2, 3]), int8());
    matches(infer([1e3, 2e3, 3e3]), int16());
    matches(infer([1e6, 2e6, 3e6]), int32());
    matches(infer([1n, 2n, 3n]), int64());

    matches(infer([-1, 2, 3]), int8());
    matches(infer([-1e3, 2e3, 3e3]), int16());
    matches(infer([-1e6, 2e6, 3e6]), int32());
    matches(infer([-1n, 2n, 3n]), int64());

    matches(infer([1, 2, null, undefined, 3]), int8());
    matches(infer([1e3, 2e3, null, undefined, 3e3]), int16());
    matches(infer([1e6, 2e6, null, undefined, 3e6]), int32());
    matches(infer([1n, 2n, null, undefined, 3n]), int64());
  });

  it('infers float types', () => {
    matches(infer([1.1, 2.2, 3.3]), float64());
    matches(infer([-1.1, 2.2, 3.3]), float64());
    matches(infer([1, 2, 3.3]), float64());
    matches(infer([1, 2, NaN]), float64());
    matches(infer([NaN, null, undefined, NaN]), float64());
    matches(infer([Number.MIN_SAFE_INTEGER, Number.MAX_SAFE_INTEGER]), float64());
  });

  it('infers utf8 dictionary types', () => {
    const type = dictionary(utf8(), int32());
    matches(infer(['foo', 'bar', 'baz']), type);
    matches(infer(['foo', 'bar', null, undefined, 'baz']), type);
  });

  it('infers bool types', () => {
    matches(infer([true, false, true]), bool());
    matches(infer([true, false, null, undefined, true]), bool());
  });

  it('infers date day types', () => {
    matches(infer([
      new Date(Date.UTC(2000, 1, 2)),
      new Date(Date.UTC(2006, 3, 20)),
      null,
      undefined
    ]), dateDay());
  });

  it('infers timestamp types', () => {
    matches(
      infer([
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
    matches(infer([[1, 2], [3, 4], [5]]), list(int8()));
    matches(infer([[true, null, false], null, undefined, [false, undefined]]), list(bool()));
    matches(infer([['foo', 'bar', null], null, ['bar', 'baz']]), list(dictionary(utf8(), int32())));
  });

  it('infers fixed size list types', () => {
    matches(
      infer([[1, 2], [3, 4]]),
      fixedSizeList(int8(), 2)
    );
    matches(
      infer([[true, null, false], null, undefined, [false, true, true]]),
      fixedSizeList(bool(), 3)
    );
    matches(
      infer([['foo', 'bar'], null, ['bar', 'baz']]),
      fixedSizeList(dictionary(utf8(), int32()), 2)
    );
  });

  it('infers struct types', () => {
    matches(
      infer([
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
    assert.throws(() => infer([(1n << 200n)]));
  });

  it('throws on mixed types', () => {
    assert.throws(() => infer([1, true, 'foo']));
  });
});
