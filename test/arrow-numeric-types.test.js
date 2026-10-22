import { describe, it, expect } from 'vitest';
import * as arrow from 'apache-arrow';
import { columnFromArray, columnFromValues, float64, int32, tableFromArrays, tableFromColumns, tableFromIPC, tableToIPC } from '../src/index.js';

const cases = [
  [new arrow.Int8(), [-128, null, 0, 127]],
  [new arrow.Int16(), [-32768, null, 0, 32767]],
  [new arrow.Int32(), [-2147483648, null, 0, 2147483647]],
  [new arrow.Int64(), [-9223372036854775808n, null, 0n, 9223372036854775807n]],
  [new arrow.Uint8(), [0, null, 1, 255]],
  [new arrow.Uint16(), [0, null, 1, 65535]],
  [new arrow.Uint32(), [0, null, 1, 4294967295]],
  [new arrow.Uint64(), [0n, null, 1n, 18446744073709551615n]],
  [new arrow.Float16(), [-1.5, null, 0, 65504]],
  [new arrow.Float32(), [-1.5, null, 0, 65504]],
  [new arrow.Float64(), [-1.25, null, 0, 1e100]]
];
const options = { useBigInt: true, maxBatchRows: 2 };

describe('Arrow JS numeric type inputs', () => {
  for (const [type, values] of cases) {
    it(`builds ${type} columns from arrays and iterables`, () => {
      Object.freeze(type);
      const column = columnFromArray(values, type, options);
      expect([...column]).toStrictEqual(values);
      expect([...columnFromValues(values.values(), type, options)]).toStrictEqual(values);
      expect(column.type).not.toBe(type);
      expect(column.type.values).toBe(type.ArrayType);
      if (type.typeId === arrow.Type.Int) {
        expect(column.type.signed).toBe(type.isSigned);
      }
    });

    for (const format of ['stream', 'file']) {
      it(`preserves ${type} values and schema in IPC ${format}`, () => {
        const table = tableFromArrays({ x: values }, { ...options, types: { x: type } });
        const bytes = tableToIPC(table, { format });
        const decoded = tableFromIPC(bytes, options).getChild('x');
        expect([...decoded]).toStrictEqual(values);
        const reference = arrow.tableFromIPC(bytes).getChild('x');
        expect([...reference]).toStrictEqual(values);
        expect(reference.type.toString()).toBe(type.toString());
      });
    }
  }

  it('preserves native type identity and typed-array input values', () => {
    for (const type of [Object.freeze(int32()), Object.freeze(float64())]) {
      const column = columnFromArray([-1, 0, 1], type);
      expect(column.type).toBe(type);
      expect([...column]).toStrictEqual([-1, 0, 1]);
    }
    const column = columnFromArray(Int32Array.of(-1000, 0, 1000), new arrow.Int32());
    expect(tableFromColumns({ x: column }).toColumns().x).toStrictEqual(Int32Array.of(-1000, 0, 1000));
  });
});
