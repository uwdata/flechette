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

/**
 * Compute a 64-bit aligned buffer size.
 * @param {number} length The starting size.
 * @param {number} bpe Bytes per element.
 * @returns {number} The aligned size.
 */
function align64(length, bpe = 1) {
  return (((length * bpe) + 7) & ~7) / bpe;
}

/**
 * Return a 64-bit aligned version of the array.
 * @template {import('../types.js').TypedArray} T
 * @param {T} array The array.
 * @param {number} length The current array length.
 * @returns {T} The aligned array.
 */
export function align(array, length = array.length) {
  const alignedLength = align64(length, array.BYTES_PER_ELEMENT);
  return array.length > alignedLength ? /** @type {T} */ (array.subarray(0, alignedLength))
    : array.length < alignedLength ? resize(array, alignedLength)
    : array;
}

/**
 * Resize a typed array to exactly the specified length.
 * @template {import('../types.js').TypedArray} T
 * @param {T} array The array.
 * @param {number} newLength The new length.
 * @returns {T} The resized array.
 */
export function resize(array, newLength) {
  // @ts-ignore
  const newArray = new array.constructor(newLength);
  newArray.set(array, array.length);
  return newArray;
}

/**
 * Grow a typed array to accommdate a minimum length. The array size is
 * doubled until it meets or exceeds the minimum length.
 * @template {import('../types.js').TypedArray} T
 * @param {T} array The array.
 * @param {number} minLength The minimum length.
 * @returns {T} The resized array.
 */
export function grow(array, minLength) {
  while (array.length < minLength) {
    array = resize(array, array.length << 1);
  }
  return array;
}
