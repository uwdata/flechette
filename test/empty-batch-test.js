import { readFileSync } from 'node:fs';
import { tableFromIPC } from '../src/index.js';
import assert from 'node:assert';

describe('Empty batch handling', () => {
  it('should decode IPC stream with empty RecordBatch', () => {
    const data = readFileSync('test/data/file-with-3-batches.bin');
    const table = tableFromIPC(data);
    
    assert.strictEqual(table.numRows, 5, 'Expected 5 total rows (3 + 0 + 2)');
    assert.strictEqual(table.numCols, 2, 'Expected 2 columns');
  });
});

