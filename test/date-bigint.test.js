import { describe, it, expect } from 'vitest';
import { DateMillisecond, Table, tableFromIPC as arrowTableFromIPC, tableToIPC as arrowTableToIPC, vectorFromArray } from 'apache-arrow';
import { columnFromArray, columnFromValues, dateMillisecond, tableFromColumns, tableFromIPC, tableToIPC } from '../src/index.js';

const options = { maxBatchRows: 3 };
const day = 86400000n;
const values = [0n, day, -day, null, 1745152496123n, -1n,
  9223372036854775807n, -9223372036854775808n];

describe('bigint Date64 inputs', () => {
  it('stores bigint inputs without conversion across builder batches', () => {
    const raw = col => col.data.flatMap(d => [...d.values.subarray(0, d.length)]);
    const expected = values.map(v => v ?? 0n);
    expect(raw(columnFromArray(values, dateMillisecond(), options))).toStrictEqual(expected);
    expect(raw(columnFromValues(values.values(), dateMillisecond(), options))).toStrictEqual(expected);
  });

  it('keeps number and Date inputs aligned to UTC days', () => {
    const input = [1745152496123, new Date(1745152496123), 1745152496123n, null];
    const col = columnFromArray(input, dateMillisecond());
    expect([...col.data[0].values]).toStrictEqual([1745107200000n, 1745107200000n, 1745152496123n, 0n]);
    expect([...col]).toStrictEqual([1745107200000, 1745107200000, 1745152496123, null]);
  });

  it('copies non-day-aligned Date64 data written by Arrow JS', () => {
    const source = arrowTableToIPC(new Table({
      date: vectorFromArray([new Date(1745152496123), null], new DateMillisecond())
    }));
    const decoded = tableFromIPC(source).getChild('date');
    const copy = columnFromArray([...decoded].map(v => v == null ? v : BigInt(v)), dateMillisecond());
    const roundtrip = tableFromIPC(tableToIPC(tableFromColumns({ date: copy })));
    expect([...roundtrip.getChild('date')]).toStrictEqual([1745152496123, null]);
  });

  for (const format of ['stream', 'file']) {
    it(`round-trips bigint inputs through IPC ${format}`, () => {
      const column = columnFromArray(values, dateMillisecond(), options);
      const bytes = tableToIPC(tableFromColumns({ date: column }), { format });

      // Arrow JS independently verifies every stored int64 value and the
      // validity bitmap.
      const arrow = arrowTableFromIPC(bytes).getChild('date');
      const raw = arrow.data.flatMap(d => [...d.values.subarray(0, d.length)]);
      expect(raw).toStrictEqual(values.map(v => v ?? 0n));
      expect(values.map((_, i) => arrow.isValid(i))).toStrictEqual(values.map(v => v !== null));

      // Flechette extracts dates as numbers, so decode the safe-integer subset.
      const safe = values.slice(0, 6);
      const safeBytes = tableToIPC(tableFromColumns({
        date: columnFromArray(safe, dateMillisecond(), options)
      }), { format });
      expect([...tableFromIPC(safeBytes).getChild('date')])
        .toStrictEqual(safe.map(v => v === null ? v : Number(v)));
    });
  }
});
