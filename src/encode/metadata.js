/**
 * @import { Builder } from './builder.js';
 */

/**
 * @param {Builder} builder
 * @param {Map<string, string>} metadata
 * @returns {number}
 */
export function encodeMetadata(builder, metadata) {
  return metadata?.size > 0
     ? builder.addOffsetVector(Array.from(metadata, ([k, v]) => {
        const key = builder.addString(`${k}`);
        const val = builder.addString(`${v}`);
        return builder.addObject(2, b => {
          b.addOffset(0, key, 0);
          b.addOffset(1, val, 0);
        });
      }))
    : 0;
}
