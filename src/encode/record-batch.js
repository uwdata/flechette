/**
 * @import { RecordBatch } from '../types.js';
 * @import { Builder } from './builder.js';
 */

/**
 * @param {Builder} builder
 * @param {RecordBatch} batch
 * @returns {number}
 */
export function encodeRecordBatch(builder, batch) {
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
    // NOT SUPPORTED: 3, compression offset
    b.addOffset(4, variadicVector, 0);
  });
}
