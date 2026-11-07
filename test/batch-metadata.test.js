import { describe, it, expect } from 'vitest';
import { RecordBatch, Table, tableFromArrays as arrowTableFromArrays, tableFromIPC as arrowTableFromIPC, tableToIPC as arrowTableToIPC } from 'apache-arrow';
import { columnFromArray, dictionary, int32, tableFromColumns, tableFromIPC, tableToIPC, utf8 } from '../src/index.js';

const md1 = new Map([['status', 'ok'], ['request_id', 'a1']]);
const md3 = new Map([['status', 'done']]);

// Build a three-batch table: x = int32, s = dictionary-encoded strings.
function flechetteTable() {
  const opt = { maxBatchRows: 2 };
  return tableFromColumns({
    x: columnFromArray([1, 2, 3, 4, 5], int32(), opt),
    s: columnFromArray(['a', 'b', 'a', 'c', 'b'], dictionary(utf8()), opt)
  });
}

// Arrow JS reference data with metadata on the first and last batches.
function arrowBytes(format) {
  const batches = [[1, 2], [3, 4], [5]].map((x, i) => {
    const batch = arrowTableFromArrays({ x: Int32Array.from(x) }).batches[0];
    const metadata = [md1, null, md3][i];
    return metadata ? new RecordBatch(batch.schema, batch.data, metadata) : batch;
  });
  return arrowTableToIPC(new Table(batches), format);
}

describe('record batch custom metadata', () => {
  for (const format of ['stream', 'file']) {
    it(`decodes metadata written by Arrow JS (${format})`, () => {
      const table = tableFromIPC(arrowBytes(format));
      expect(table.batchMetadata).toStrictEqual([md1, null, md3]);
      expect(table.toColumns().x).toStrictEqual(Int32Array.of(1, 2, 3, 4, 5));
    });

    it(`encodes metadata readable by Arrow JS (${format})`, () => {
      const bytes = tableToIPC(flechetteTable(), {
        format, batchMetadata: [md1, undefined, md3]
      });
      const arrow = arrowTableFromIPC(bytes);
      expect(arrow.batches.map(b => b.metadata)).toStrictEqual([md1, new Map, md3]);
      expect(arrow.getChild('s').toArray()).toStrictEqual(['a', 'b', 'a', 'c', 'b']);
    });

    it(`round-trips table batch metadata by default (${format})`, () => {
      const table = tableFromIPC(arrowBytes(format));
      const decoded = tableFromIPC(tableToIPC(table, { format }));
      expect(decoded.batchMetadata).toStrictEqual([md1, null, md3]);
    });
  }

  it('leaves batch metadata null when absent', () => {
    const table = flechetteTable();
    expect(table.batchMetadata).toBe(null);
    const decoded = tableFromIPC(tableToIPC(table));
    expect(decoded.batchMetadata).toBe(null);
  });

  it('writes the batchMetadata property of constructed tables', () => {
    const table = flechetteTable();
    table.batchMetadata = [null, md1];
    const decoded = tableFromIPC(tableToIPC(table));
    expect(decoded.batchMetadata).toStrictEqual([null, md1, null]);
  });

  it('prefers the batchMetadata option over the table property', () => {
    const table = flechetteTable();
    table.batchMetadata = [md1];
    const decoded = tableFromIPC(tableToIPC(table, { batchMetadata: [null, null, md3] }));
    expect(decoded.batchMetadata).toStrictEqual([null, null, md3]);
  });

  it('preserves batch metadata when selecting columns', () => {
    const table = tableFromIPC(arrowBytes('stream'));
    expect(table.select(['x'], ['y']).batchMetadata).toStrictEqual([md1, null, md3]);
  });

  it('rejects more metadata entries than record batches', () => {
    expect(() => tableToIPC(flechetteTable(), { batchMetadata: [md1, md1, md1, md1] }))
      .toThrow('Batch metadata has 4 entries, but the table has 3 record batches.');
  });
});
