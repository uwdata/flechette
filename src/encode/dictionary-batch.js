import { encodeRecordBatch } from './record-batch.js';

export function encodeDictionaryBatch(builder, dictionaryBatch) {
  const dataOffset = encodeRecordBatch(builder, dictionaryBatch.data);
  return builder.addObject(3, b => {
    b.addInt64(0, dictionaryBatch.id, 0);
    b.addOffset(1, dataOffset, 0);
    b.addInt8(2, +dictionaryBatch.isDelta, 0);
  });
}
