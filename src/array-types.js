export const uint8 = Uint8Array;
export const uint16 = Uint16Array;
export const uint32 = Uint32Array;
export const uint64 = BigUint64Array;
export const int8 = Int8Array;
export const int16 = Int16Array;
export const int32 = Int32Array;
export const int64 = BigInt64Array;
export const float32 = Float32Array;
export const float64 = Float64Array;

/**
 * Return the appropriate typed array constructor for the given
 * integer type metadata.
 * @param {number} bitWidth The integer size in bits.
 * @param {boolean} signed Flag indicating if the integer is signed.
 * @returns {import('./types.js').IntArrayConstructor}
 */
export function arrayTypeInt(bitWidth, signed) {
  const i = Math.log2(bitWidth) - 3;
  return (
    signed ? [int8, int16, int32, int64] : [uint8, uint16, uint32, uint64]
  )[i];
}
