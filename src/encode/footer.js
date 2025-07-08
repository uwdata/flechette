/**
 * @import { Block, Schema } from '../types.js';
 * @import { Builder } from './builder.js';
 */
import { MAGIC, Version } from '../constants.js';
import { encodeMetadata } from './metadata.js';
import { encodeSchema } from './schema.js';

/**
 * Write a file footer.
 * @param {Builder} builder The binary builder.
 * @param {Schema} schema The table schema.
 * @param {Block[]} dictBlocks Dictionary batch file blocks.
 * @param {Block[]} recordBlocks Record batch file blocks.
 * @param {Map<string,string> | null} metadata File-level metadata.
 */
export function writeFooter(builder, schema, dictBlocks, recordBlocks, metadata) {
  // encode footer flatbuffer
  const metadataOffset = encodeMetadata(builder, metadata);
  const recsOffset = builder.addVector(recordBlocks, 24, 8, encodeBlock);
  const dictsOffset = builder.addVector(dictBlocks, 24, 8, encodeBlock);
  const schemaOffset = encodeSchema(builder, schema);
  builder.finish(
    builder.addObject(5, b => {
      b.addInt16(0, Version.V5, Version.V1);
      b.addOffset(1, schemaOffset, 0);
      b.addOffset(2, dictsOffset, 0);
      b.addOffset(3, recsOffset, 0);
      b.addOffset(4, metadataOffset, 0);
    })
  );
  const size = builder.offset();

  // add eos with continuation indicator
  builder.addInt32(0);
  builder.addInt32(-1);

  // write builder contents
  builder.flush();

  // write file tail
  builder.sink.write(new Uint8Array(Int32Array.of(size).buffer));
  builder.sink.write(MAGIC);
}

/**
 * Encode a file pointer block.
 * @param {Builder} builder
 * @param {Block} block
 * @returns {number} the current block offset
 */
function encodeBlock(builder, { offset, metadataLength, bodyLength }) {
  builder.writeInt64(bodyLength);
  builder.writeInt32(0);
  builder.writeInt32(metadataLength);
  builder.writeInt64(offset);
  return builder.offset();
}
