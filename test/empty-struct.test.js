import { describe, it, expect } from "vitest";
import { readFile } from 'node:fs/promises';
import { tableFromIPC, tableToIPC } from '../src/index.js';

describe('Empty struct handling', () => {
  it('should decode IPC stream with empty struct column', async () => {
    const data = await readFile('test/data/empty_struct.arrows');
    const table = tableFromIPC(data);
    
    expect(table.numRows).toBe(3);
    expect(table.numCols).toBe(3);
    
    const rows = table.toArray();
    expect(rows[0]).toStrictEqual({ before: 1, empty: {}, after: 4 });
    expect(rows[1]).toStrictEqual({ before: 2, empty: {}, after: 5 });
    expect(rows[2]).toStrictEqual({ before: 3, empty: {}, after: 6 });
  });

  it('should decode IPC file with empty struct column', async () => {
    const data = await readFile('test/data/empty_struct.arrow');
    const table = tableFromIPC(data);
    
    expect(table.numRows).toBe(3);
    expect(table.numCols).toBe(3);
    
    const rows = table.toArray();
    expect(rows[0]).toStrictEqual({ before: 1, empty: {}, after: 4 });
    expect(rows[1]).toStrictEqual({ before: 2, empty: {}, after: 5 });
    expect(rows[2]).toStrictEqual({ before: 3, empty: {}, after: 6 });
  });

  it('should encode and round-trip IPC stream with empty struct', async () => {
    const original = tableFromIPC(await readFile('test/data/empty_struct.arrows'));
    const encoded = tableToIPC(original, { format: 'stream' });
    const decoded = tableFromIPC(encoded);

    expect(decoded.numRows).toBe(3);
    expect(decoded.numCols).toBe(3);
    expect(decoded.toArray()).toStrictEqual(original.toArray());
  });

  it('should encode and round-trip IPC file with empty struct', async () => {
    const original = tableFromIPC(await readFile('test/data/empty_struct.arrow'));
    const encoded = tableToIPC(original, { format: 'file' });
    const decoded = tableFromIPC(encoded);

    expect(decoded.numRows).toBe(3);
    expect(decoded.numCols).toBe(3);
    expect(decoded.toArray()).toStrictEqual(original.toArray());
  });
});

