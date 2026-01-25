/**
 * @import { BodyCompression, RecordBatch } from '../types.js';
 * @import { Builder } from './builder.js';
 */
import { BodyCompressionMethod, CompressionType } from '../constants.js';

/**
 * @param {Builder} builder
 * @param {RecordBatch} batch
 * @param {BodyCompression | null} [compression]
 * @returns {number}
 */
export function encodeRecordBatch(builder, batch, compression) {
  const { nodes, regions, variadic } = batch;
  const nodeVector = builder.addVector(nodes, 16, 8,
    (builder, node) => {
      builder.writeInt64(node.nullCount);
      builder.writeInt64(node.length);
      return builder.offset();
    }
  );
  const regionVector = builder.addVector(regions, 16, 8,
    (builder, region) => {
      builder.writeInt64(region.length);
      builder.writeInt64(region.offset);
      return builder.offset();
    }
  );
  const variadicVector = builder.addVector(variadic, 8, 8,
    (builder, count) => builder.addInt64(count)
  );
  return builder.addObject(5, b => {
    b.addInt64(0, nodes[0].length, 0);
    b.addOffset(1, nodeVector, 0);
    b.addOffset(2, regionVector, 0);
    b.addOffset(3, encodeCompression(builder, compression), 0);
    b.addOffset(4, variadicVector, 0);
  });
}

/**
 * @param {Builder} builder
 * @param {BodyCompression | null} [compression]
 * @returns {number}
 */
function encodeCompression(builder, compression) {
  if (!compression) return 0;
  const { codec, method } = compression;
  return builder.addObject(2, b => {
    b.addInt8(0, codec, CompressionType.LZ4_FRAME);
    b.addInt8(1, method, BodyCompressionMethod.BUFFER);
  });
}
