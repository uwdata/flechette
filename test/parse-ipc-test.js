import assert from 'node:assert';
import { readFile } from 'node:fs/promises';
import { Type, Version } from '../src/index.js';
import { parseIPC } from '../src/parse-ipc.js';

function decimalData() {
  return {
    schema: {
      version: Version.V5,
      endianness: 0,
      fields: [{
        name: 'd',
        type: { typeId: Type.Decimal, precision: 18, scale: 3, bitWidth: 128, values: Uint32Array },
        nullable: true,
        metadata: null
      }],
      metadata: null,
      dictionaryTypes: new Map
    },
    records: [{
      length: 3,
      nodes: [ { length: 3, nullCount: 0 } ],
      buffers: [
        { offset: 0, length: 0 },
        { offset: 0, length: 48 }
      ],
      variadic: [],
      body: Uint8Array.of(232,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,224,46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,208,132,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
    }],
    dictionaries: [],
    metadata: null
  };
}

describe('parseIPC', () => {
  it('decodes arrow file format', async () => {
    const buffer = await readFile(`test/data/decimal.arrow`);
    const bytes = new Uint8Array(buffer);
    const expect = decimalData();
    assert.deepEqual(parseIPC(buffer), expect, 'Node Buffer');
    assert.deepStrictEqual(parseIPC(bytes), expect, 'Uint8Array');
    assert.deepStrictEqual(parseIPC(bytes.buffer), expect, 'ArrayBuffer');
  });

  it('decodes arrow stream format', async () => {
    const buffer = await readFile(`test/data/decimal.arrows`);
    const bytes = new Uint8Array(buffer);
    const expect = decimalData();
    assert.deepEqual(parseIPC(buffer), expect, 'Node Buffer');
    assert.deepStrictEqual(parseIPC(bytes), expect, 'Uint8Array');
    assert.deepStrictEqual(parseIPC(bytes.buffer), expect, 'ArrayBuffer');
  });

  it('decodes arrow stream format from multiple buffers', async () => {
    // decimal.arrows, divided into separate messages
    const array = [
      Uint8Array.of(255,255,255,255,120,0,0,0,16,0,0,0,0,0,10,0,12,0,6,0,5,0,8,0,10,0,0,0,0,1,4,0,12,0,0,0,8,0,8,0,0,0,4,0,8,0,0,0,4,0,0,0,1,0,0,0,20,0,0,0,16,0,20,0,8,0,6,0,7,0,12,0,0,0,16,0,16,0,0,0,0,0,1,7,16,0,0,0,28,0,0,0,4,0,0,0,0,0,0,0,1,0,0,0,100,0,0,0,8,0,12,0,4,0,8,0,8,0,0,0,18,0,0,0,3,0,0,0),
      Uint8Array.of(255,255,255,255,136,0,0,0,20,0,0,0,0,0,0,0,12,0,22,0,6,0,5,0,8,0,12,0,12,0,0,0,0,3,4,0,24,0,0,0,48,0,0,0,0,0,0,0,0,0,10,0,24,0,12,0,4,0,8,0,10,0,0,0,60,0,0,0,16,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,2,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,48,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,232,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,224,46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,208,132,0,0,0,0,0,0,0,0,0,0,0,0,0,0),
      Uint8Array.of(255,255,255,255,0,0,0,0)
    ];
    const expect = decimalData();
    assert.deepStrictEqual(parseIPC(array), expect, 'Uint8Array');
  });
});
