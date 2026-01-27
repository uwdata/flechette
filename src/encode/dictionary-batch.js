/**
 * @import { BodyCompression, DictionaryBatch } from '../types.js';
 * @import { Builder } from './builder.js';
 */
import { encodeRecordBatch } from './record-batch.js';

/**
 * @param {Builder} builder
 * @param {DictionaryBatch} dictionaryBatch
 * @param {BodyCompression | null} compression
 * @returns {number}
 */
export function encodeDictionaryBatch(builder, dictionaryBatch, compression) {
  const dataOffset = encodeRecordBatch(builder, dictionaryBatch.data, compression);
  return builder.addObject(3, b => {
    b.addInt64(0, dictionaryBatch.id, 0);
    b.addOffset(1, dataOffset, 0);
    b.addInt8(2, +dictionaryBatch.isDelta, 0);
  });
}
