import { describe, it, expect } from "vitest";
import { readFileSync } from 'node:fs';
import { tableFromIPC } from '../src/index.js';

describe('Empty batch handling', () => {
  it('should decode IPC stream with empty RecordBatch', () => {
    const data = readFileSync('test/data/file-with-3-batches.bin');
    const table = tableFromIPC(data);
    
    expect(table.numRows).toBe(5); // Expect 5 total rows (3 + 0 + 2)
    expect(table.numCols).toBe(2); // Expect 2 columns
  });
});
