import { Type, Version } from '../../src/index.js';

export function decimalDataToEncode() {
  return {
    schema: {
      version: Version.V5,
      endianness: 0,
      fields: [{
        name: 'd',
        type: { typeId: Type.Decimal, precision: 18, scale: 3, bitWidth: 128, values: BigUint64Array },
        nullable: true,
        metadata: null
      }],
      metadata: null
    },
    records: [{
      length: 3,
      nodes: [ { length: 3, nullCount: 0 } ],
      regions: [
        { offset: 0, length: 0 },
        { offset: 0, length: 48 }
      ],
      variadic: [],
      buffers: [
        Uint8Array.of(232,3,0,0,0,0,0,0,0,0,0,0,0,0,0,0,224,46,0,0,0,0,0,0,0,0,0,0,0,0,0,0,208,132,0,0,0,0,0,0,0,0,0,0,0,0,0,0)
      ],
      byteLength: 48
    }],
    dictionaries: [],
    metadata: null
  };
}

export function decimalDataDecoded() {
  const data = decimalDataToEncode();
  const record = data.records[0];
  record.body = record.buffers[0];
  delete record.byteLength;
  delete record.buffers;
  return data;
}
