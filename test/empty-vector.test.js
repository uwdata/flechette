import { describe, it, expect } from 'vitest';
import { tableFromIPC as arrowTableFromIPC } from 'apache-arrow';
import { columnFromArray, struct, tableFromColumns, tableFromIPC, tableToIPC } from '../src/index.js';
import { Builder } from '../src/encode/builder.js';
import { MemorySink } from '../src/encode/sink.js';

// Inspect the wire representation: readers often treat an absent vector and
// a present empty vector identically, hiding invalid schema fields.
function fieldOffset(view, table, field) {
  const vtable = table - view.getInt32(table, true);
  const slot = 4 + 2 * field;
  return slot < view.getUint16(vtable, true) ? view.getUint16(vtable + slot, true) : 0;
}

function indirect(view, offset) {
  return offset + view.getUint32(offset, true);
}

describe('empty FlatBuffer vectors', () => {
  for (const alignment of [1, 4, 8]) {
    it(`encodes an empty vector with ${alignment}-byte element alignment`, () => {
      const sink = new MemorySink();
      const builder = new Builder(sink);
      builder.addInt8(42); // force alignment padding before the vector
      const vector = builder.addVector([], alignment, alignment, () => {
        throw new Error('An empty vector must not write any elements.');
      });
      builder.finish(builder.addObject(1, b => b.addOffset(0, vector, 0)));
      builder.flush();
      const bytes = sink.finish();
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const root = indirect(view, 0);
      const offset = fieldOffset(view, root, 0);
      expect(offset).toBeGreaterThan(0);
      const start = indirect(view, root + offset);
      expect(view.getUint32(start, true)).toBe(0);
      expect((start + 4) % alignment).toBe(0);
    });
  }

  it('keeps absent optional vectors absent', () => {
    const builder = new Builder(new MemorySink());
    expect(builder.addVector(undefined, 8, 8, () => {})).toBe(0);
    expect(builder.addVector(null, 8, 8, () => {})).toBe(0);
  });

  for (const format of ['stream', 'file']) {
    it(`writes a present empty schema field vector in IPC ${format}`, () => {
      const bytes = tableToIPC(tableFromColumns({}), { format });
      const view = new DataView(bytes.buffer, bytes.byteOffset, bytes.byteLength);
      const message = indirect(view, format === 'file' ? 16 : 8);
      const schema = indirect(view, message + fieldOffset(view, message, 2));
      const schemas = [schema];
      if (format === 'file') {
        const footerStart = bytes.length - 10 - view.getInt32(bytes.length - 10, true);
        const footer = indirect(view, footerStart);
        schemas.push(indirect(view, footer + fieldOffset(view, footer, 1)));
      }
      for (const table of schemas) {
        const offset = fieldOffset(view, table, 1);
        expect(offset).toBeGreaterThan(0);
        expect(view.getUint32(indirect(view, table + offset), true)).toBe(0);
        expect(fieldOffset(view, table, 2)).toBe(0); // optional metadata stays absent
      }
      for (const decoded of [tableFromIPC(bytes), arrowTableFromIPC(bytes)]) {
        expect(decoded.numCols).toBe(0);
        expect(decoded.numRows).toBe(0);
      }
    });

    it(`round-trips empty struct children in IPC ${format}`, () => {
      const values = [{}, null, {}];
      const column = columnFromArray(values, struct([]));
      const bytes = tableToIPC(tableFromColumns({ empty: column }), { format });
      expect(tableFromIPC(bytes).toColumns().empty).toStrictEqual(values);
      const arrow = arrowTableFromIPC(bytes).getChild('empty');
      expect([...arrow].map(v => v === null ? null : v.toJSON())).toStrictEqual(values);
    });
  }
});
