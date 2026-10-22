import { describe, it, expect } from 'vitest';
import { tableFromIPC as arrowTableFromIPC } from 'apache-arrow';
import { TimeUnit, columnFromArray, columnFromValues, tableFromColumns, tableFromIPC, tableToIPC, timestamp } from '../src/index.js';

const options = { useBigIntTimestamp: true, maxBatchRows: 3 };
const values = [0n, 1n, -1n, null, 1700000000000000001n, -1700000000000000001n,
  9223372036854775807n, -9223372036854775808n];

describe('bigint timestamp inputs', () => {
  for (const [name, unit, scale] of [
    ['second', TimeUnit.SECOND, 0.001],
    ['millisecond', TimeUnit.MILLISECOND, 1],
    ['microsecond', TimeUnit.MICROSECOND, 1000],
    ['nanosecond', TimeUnit.NANOSECOND, 1000000]
  ]) {
    it(`preserves native ${name} values across builder batches`, () => {
      const type = timestamp(unit, 'UTC');
      expect([...columnFromArray(values, type, options)]).toStrictEqual(values);
      expect([...columnFromValues(values.values(), type, options)]).toStrictEqual(values);
      const typed = BigInt64Array.from(values.filter(v => v !== null));
      expect([...columnFromArray(typed, type, options)]).toStrictEqual([...typed]);
    });

    it(`keeps number and Date inputs in milliseconds for ${name} columns`, () => {
      const input = [1000, new Date(-1000), 1n, null];
      const col = columnFromArray(input, timestamp(unit), options);
      expect([...col]).toStrictEqual([BigInt(1000 * scale), BigInt(-1000 * scale), 1n, null]);
      expect([...columnFromArray([2n], timestamp(unit))]).toStrictEqual([2 / scale]);
    });

    for (const format of ['stream', 'file']) {
      it(`round-trips native ${name} values through IPC ${format}`, () => {
        const column = columnFromArray(values, timestamp(unit, 'UTC'), options);
        const bytes = tableToIPC(tableFromColumns({ time: column }), { format });
        const decoded = tableFromIPC(bytes, options).getChild('time');
        expect([...decoded]).toStrictEqual(values);
        expect(decoded.type.unit).toBe(unit);
        expect(decoded.type.timezone).toBe('UTC');

        // Arrow JS getters convert to milliseconds; inspect its decoded int64
        // buffers to check every stored bit, including sub-millisecond values.
        const arrow = arrowTableFromIPC(bytes).getChild('time');
        const raw = arrow.data.flatMap(d => [...d.values.subarray(0, d.length)]);
        expect(raw).toStrictEqual(values.map(v => v ?? 0n));
        expect(values.map((_, i) => arrow.isValid(i))).toStrictEqual(values.map(v => v !== null));
      });
    }
  }
});
