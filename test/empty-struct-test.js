import { readFileSync } from 'node:fs';
import { tableFromIPC } from '../src/index.js';
import assert from 'node:assert';

describe('Empty struct handling', () => {
  it('should decode IPC stream with empty struct column', () => {
    const data = readFileSync('test/data/empty_struct.arrows');
    const table = tableFromIPC(data);
    
    assert.strictEqual(table.numRows, 3);
    assert.strictEqual(table.numCols, 3);
    
    const rows = table.toArray();
    assert.deepStrictEqual(rows[0], { before: 1, empty: {}, after: 4 });
    assert.deepStrictEqual(rows[1], { before: 2, empty: {}, after: 5 });
    assert.deepStrictEqual(rows[2], { before: 3, empty: {}, after: 6 });
  });

  it('should decode IPC file with empty struct column', () => {
    const data = readFileSync('test/data/empty_struct.arrow');
    const table = tableFromIPC(data);
    
    assert.strictEqual(table.numRows, 3);
    assert.strictEqual(table.numCols, 3);
    
    const rows = table.toArray();
    assert.deepStrictEqual(rows[0], { before: 1, empty: {}, after: 4 });
    assert.deepStrictEqual(rows[1], { before: 2, empty: {}, after: 5 });
    assert.deepStrictEqual(rows[2], { before: 3, empty: {}, after: 6 });
  });
});

