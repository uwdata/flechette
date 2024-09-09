export const uint8Array = Uint8Array;
export const uint16Array = Uint16Array;
export const uint32Array = Uint32Array;
export const uint64Array = BigUint64Array;
export const int8Array = Int8Array;
export const int16Array = Int16Array;
export const int32Array = Int32Array;
export const int64Array = BigInt64Array;
export const float32Array = Float32Array;
export const float64Array = Float64Array;

/**
 * Return the appropriate typed array constructor for the given
 * integer type metadata.
 * @param {number} bitWidth The integer size in bits.
 * @param {boolean} signed Flag indicating if the integer is signed.
 * @returns {import('../types.js').IntArrayConstructor}
 */
export function intArrayType(bitWidth, signed) {
  const i = Math.log2(bitWidth) - 3;
  return (
    signed
      ? [int8Array, int16Array, int32Array, int64Array]
      : [uint8Array, uint16Array, uint32Array, uint64Array]
  )[i];
}

/** Shared prototype for typed arrays. */
const TypedArray = Object.getPrototypeOf(Int8Array);

/**
 * Check if a value is a typed array.
 * @param {*} value The value to check.
 * @returns {value is import('../types.js').TypedArray}
 *  True if value is a typed array, false otherwise.
 */
export function isTypedArray(value) {
  return value instanceof TypedArray;
}

/**
 * Check if a value is either a standard array or typed array.
 * @param {*} value The value to check.
 * @returns {value is (Array | import('../types.js').TypedArray)}
 *  True if value is an array, false otherwise.
 */
export function isArray(value) {
  return Array.isArray(value) || isTypedArray(value);
}

/**
 * Check if a value is an array type (constructor) for 64-bit integers,
 * one of BigInt64Array or BigUint64Array.
 * @param {*} value The value to check.
 * @returns {value is import('../types.js').Int64ArrayConstructor}
 *  True if value is a 64-bit array type, false otherwise.
 */
export function isInt64ArrayType(value) {
  return value === int64Array || value === uint64Array;
}

/**
 * Determine the correct index into an offset array for a given
 * full column row index. Assumes offset indices can be manipulated
 * as 32-bit signed integers.
 * @param {import('../types.js').IntegerArray} offsets The offsets array.
 * @param {number} index The full column row index.
 */
export function bisect(offsets, index) {
  let a = 0;
  let b = offsets.length;
  if (b <= 2147483648) { // 2 ** 31
    // fast version, use unsigned bit shift
    // array length fits within 32-bit signed integer
    do {
      const mid = (a + b) >>> 1;
      if (offsets[mid] <= index) a = mid + 1;
      else b = mid;
    } while (a < b);
  } else {
    // slow version, use division and truncate
    // array length exceeds 32-bit signed integer
    do {
      const mid = Math.trunc((a + b) / 2);
      if (offsets[mid] <= index) a = mid + 1;
      else b = mid;
    } while (a < b);
  }
  return a;
}
