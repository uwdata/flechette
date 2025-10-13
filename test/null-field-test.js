import assert from 'node:assert';
import { readFile } from 'node:fs/promises';
import { tableFromColumns, columnFromArray, tableFromIPC, tableToIPC } from '../src/index.js';

describe('Null field compatibility', () => {
  it('reads PyArrow-generated files with null columns', async () => {
    // Test file format
    const fileBytes = new Uint8Array(await readFile('test/data/null_test.arrow'));
    const fileTable = tableFromIPC(fileBytes);
    
    assert.strictEqual(fileTable.numRows, 4);
    assert.strictEqual(fileTable.numCols, 3);
    
    // Check schema
    const fields = fileTable.schema.fields;
    assert.strictEqual(fields.length, 3);
    assert.strictEqual(fields[0].name, 'numbers');
    assert.strictEqual(fields[1].name, 'text');
    assert.strictEqual(fields[2].name, 'nulls');
    assert.strictEqual(fields[2].type.typeId, 1); // Type.Null
    
    // Check data
    const numbers = fileTable.getChildAt(0).toArray();
    const text = fileTable.getChildAt(1).toArray();
    const nulls = fileTable.getChildAt(2).toArray();
    
    assert.deepStrictEqual(Array.from(numbers), [1, 2, 3, 4]);
    assert.deepStrictEqual(Array.from(text), ['hello', 'world', 'test', 'null']);
    assert.deepStrictEqual(Array.from(nulls), [null, null, null, null]);
    
    // Test stream format
    const streamBytes = new Uint8Array(await readFile('test/data/null_test.arrows'));
    const streamTable = tableFromIPC(streamBytes);
    
    assert.strictEqual(streamTable.numRows, 4);
    assert.strictEqual(streamTable.numCols, 3);
  });

  it('creates files with null columns that PyArrow can read', () => {
    // Create table with null column
    const intCol = columnFromArray([10, 20, 30]);
    const strCol = columnFromArray(['a', 'b', 'c']); 
    const nullCol = columnFromArray([null, null, null]);
    
    const table = tableFromColumns({ 
      integers: intCol, 
      strings: strCol, 
      nulls: nullCol 
    });
    
    // Test both formats
    const fileBytes = tableToIPC(table, { format: 'file' });
    const streamBytes = tableToIPC(table, { format: 'stream' });
    
    // Round-trip test
    const fileTable = tableFromIPC(fileBytes);
    const streamTable = tableFromIPC(streamBytes);
    
    // Verify structure
    assert.strictEqual(fileTable.numRows, 3);
    assert.strictEqual(fileTable.numCols, 3);
    assert.strictEqual(streamTable.numRows, 3);
    assert.strictEqual(streamTable.numCols, 3);
    
    // Verify null column is preserved
    const nullField = fileTable.schema.fields.find(f => f.name === 'nulls');
    assert.strictEqual(nullField.type.typeId, 1); // Type.Null
    
    const nullData = fileTable.getChild('nulls').toArray();
    assert.deepStrictEqual(Array.from(nullData), [null, null, null]);
  });

  it('handles mixed null and non-null data correctly', () => {
    // Test columns with some null values (different from pure null columns)
    const mixedCol = columnFromArray([1, null, 3, null, 5]);
    const pureNullCol = columnFromArray([null, null, null, null, null]);
    
    const table = tableFromColumns({
      mixed: mixedCol,
      pure_nulls: pureNullCol
    });
    
    const bytes = tableToIPC(table, { format: 'file' });
    const roundTrip = tableFromIPC(bytes);
    
    // Verify the mixed column (should not be Type.Null)
    const mixedField = roundTrip.schema.fields.find(f => f.name === 'mixed');
    assert.notStrictEqual(mixedField.type.typeId, 1); // Should be Int type, not Null
    
    // Verify the pure null column (should be Type.Null)
    const nullField = roundTrip.schema.fields.find(f => f.name === 'pure_nulls');
    assert.strictEqual(nullField.type.typeId, 1); // Should be Type.Null
    
    // Verify data integrity
    const mixedData = roundTrip.getChild('mixed').toArray();
    const nullData = roundTrip.getChild('pure_nulls').toArray();
    
    assert.deepStrictEqual(Array.from(mixedData), [1, null, 3, null, 5]);
    assert.deepStrictEqual(Array.from(nullData), [null, null, null, null, null]);
  });

  it('handles null-type column NOT in last position (field node alignment bug)', async () => {
    // Regression test: 2.2.4 added null column support (7b9b5eb), but the encoder/decoder
    // got out of sync. Encoder wrote field nodes for null columns, decoder didn't read them.
    // Result: columns after null-type columns read wrong field nodes and got corrupted.

    // Test data generated with flechette itself, contains:
    // - strings column: ['s1', 's2', 's3']
    // - nulls column: [null, null, null] (Type.Null)
    // - floats column: [3.14, 3.14, 3.14]
    const bytes = new Uint8Array(await readFile('test/data/null_not_last.arrows'));
    const table = tableFromIPC(bytes);

    assert.strictEqual(table.numRows, 3);
    assert.strictEqual(table.numCols, 3);

    const fields = table.schema.fields;
    assert.strictEqual(fields[0].name, 'strings');
    assert.strictEqual(fields[1].name, 'nulls');
    assert.strictEqual(fields[2].name, 'floats');
    assert.strictEqual(fields[1].type.typeId, 1); // Type.Null

    const strings = table.getChild('strings');
    const nulls = table.getChild('nulls');
    const floats = table.getChild('floats');

    // Validate strings column
    assert.deepStrictEqual(Array.from(strings.toArray()), ['s1', 's2', 's3']);
    assert.strictEqual(strings.data[0].nullCount, 0);

    // Validate nulls column
    assert.deepStrictEqual(Array.from(nulls.toArray()), [null, null, null]);
    assert.strictEqual(nulls.data[0].nullCount, 3);

    // Critical: floats column after null-type column must decode correctly
    // Bug would cause: nullCount=3, values=[null, null, null]
    assert.strictEqual(floats.data[0].nullCount, 0);
    assert.strictEqual(floats.data[0].length, 3);
    assert.deepStrictEqual(Array.from(floats.toArray()), [3.14, 3.14, 3.14]);
  });

  it('handles multiple consecutive null-type columns', () => {
    // Edge case: multiple null-type columns in a row
    const table = tableFromColumns({
      str1: columnFromArray(['a', 'b']),
      null1: columnFromArray([null, null]),
      null2: columnFromArray([null, null]),
      float: columnFromArray([3.14, 3.14])
    });

    const bytes = tableToIPC(table, { format: 'stream' });
    const decoded = tableFromIPC(bytes);

    assert.strictEqual(decoded.numRows, 2);
    assert.strictEqual(decoded.numCols, 4);

    // All columns must decode correctly despite two null columns in between
    const str1 = decoded.getChild('str1');
    const null1 = decoded.getChild('null1');
    const null2 = decoded.getChild('null2');
    const float = decoded.getChild('float');

    assert.deepStrictEqual(Array.from(str1.toArray()), ['a', 'b']);
    assert.strictEqual(str1.data[0].nullCount, 0);

    assert.deepStrictEqual(Array.from(null1.toArray()), [null, null]);
    assert.strictEqual(null1.data[0].nullCount, 2);

    assert.deepStrictEqual(Array.from(null2.toArray()), [null, null]);
    assert.strictEqual(null2.data[0].nullCount, 2);

    assert.deepStrictEqual(Array.from(float.toArray()), [3.14, 3.14]);
    assert.strictEqual(float.data[0].nullCount, 0);
  });
});
