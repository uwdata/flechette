import { describe, it, expect } from "vitest";
import { readFile } from 'node:fs/promises';
import { tableFromColumns, columnFromArray, tableFromIPC, tableToIPC } from '../src/index.js';

describe('Null field compatibility', () => {
  it('reads PyArrow-generated files with null columns', async () => {
    // Test file format
    const fileBytes = new Uint8Array(await readFile('test/data/null_test.arrow'));
    const fileTable = tableFromIPC(fileBytes);
    
    expect(fileTable.numRows).toBe(4);
    expect(fileTable.numCols).toBe(3);
    
    // Check schema
    const fields = fileTable.schema.fields;
    expect(fields.length).toBe(3);
    expect(fields[0].name).toBe('numbers');
    expect(fields[1].name).toBe('text');
    expect(fields[2].name).toBe('nulls');
    expect(fields[2].type.typeId).toBe(1); // Type.Null
    
    // Check data
    const numbers = fileTable.getChildAt(0).toArray();
    const text = fileTable.getChildAt(1).toArray();
    const nulls = fileTable.getChildAt(2).toArray();
    
    expect(Array.from(numbers)).toStrictEqual([1, 2, 3, 4]);
    expect(Array.from(text)).toStrictEqual(['hello', 'world', 'test', 'null']);
    expect(Array.from(nulls)).toStrictEqual([null, null, null, null]);
    
    // Test stream format
    const streamBytes = new Uint8Array(await readFile('test/data/null_test.arrows'));
    const streamTable = tableFromIPC(streamBytes);
    
    expect(streamTable.numRows).toBe(4);
    expect(streamTable.numCols).toBe(3);
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
    expect(fileTable.numRows).toBe(3);
    expect(fileTable.numCols).toBe(3);
    expect(streamTable.numRows).toBe(3);
    expect(streamTable.numCols).toBe(3);
    
    // Verify null column is preserved
    const nullField = fileTable.schema.fields.find(f => f.name === 'nulls');
    expect(nullField.type.typeId).toBe(1); // Type.Null
    
    const nullData = fileTable.getChild('nulls').toArray();
    expect(Array.from(nullData)).toStrictEqual([null, null, null]);
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
    expect(mixedField.type.typeId).not.toBe(1); // Should be Int type, not Null
    
    // Verify the pure null column (should be Type.Null)
    const nullField = roundTrip.schema.fields.find(f => f.name === 'pure_nulls');
    expect(nullField.type.typeId).toBe(1); // Should be Type.Null
    
    // Verify data integrity
    const mixedData = roundTrip.getChild('mixed').toArray();
    const nullData = roundTrip.getChild('pure_nulls').toArray();
    
    expect(Array.from(mixedData)).toStrictEqual([1, null, 3, null, 5]);
    expect(Array.from(nullData)).toStrictEqual([null, null, null, null, null]);
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

    expect(table.numRows).toBe(3);
    expect(table.numCols).toBe(3);

    const fields = table.schema.fields;
    expect(fields[0].name).toBe('strings');
    expect(fields[1].name).toBe('nulls');
    expect(fields[2].name).toBe('floats');
    expect(fields[1].type.typeId).toBe(1); // Type.Null

    const strings = table.getChild('strings');
    const nulls = table.getChild('nulls');
    const floats = table.getChild('floats');

    // Validate strings column
    expect(Array.from(strings.toArray())).toStrictEqual(['s1', 's2', 's3']);
    expect(strings.data[0].nullCount).toBe(0);

    // Validate nulls column
    expect(Array.from(nulls.toArray())).toStrictEqual([null, null, null]);
    expect(nulls.data[0].nullCount).toBe(3);

    // Critical: floats column after null-type column must decode correctly
    // Bug would cause: nullCount=3, values=[null, null, null]
    expect(floats.data[0].nullCount).toBe(0);
    expect(floats.data[0].length).toBe(3);
    expect(Array.from(floats.toArray())).toStrictEqual([3.14, 3.14, 3.14]);
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

    expect(decoded.numRows).toBe(2);
    expect(decoded.numCols).toBe(4);

    // All columns must decode correctly despite two null columns in between
    const str1 = decoded.getChild('str1');
    const null1 = decoded.getChild('null1');
    const null2 = decoded.getChild('null2');
    const float = decoded.getChild('float');

    expect(Array.from(str1.toArray())).toStrictEqual(['a', 'b']);
    expect(str1.data[0].nullCount).toBe(0);

    expect(Array.from(null1.toArray())).toStrictEqual([null, null]);
    expect(null1.data[0].nullCount).toBe(2);

    expect(Array.from(null2.toArray())).toStrictEqual([null, null]);
    expect(null2.data[0].nullCount).toBe(2);

    expect(Array.from(float.toArray())).toStrictEqual([3.14, 3.14]);
    expect(float.data[0].nullCount).toBe(0);
  });
});
