/**
 * @import { TimeUnit_, TypedArray } from '../types.js';
 */
import { float64Array, int32Array, int64Array, isInt64ArrayType, uint32Array, uint8Array } from './arrays.js';
import { TimeUnit } from '../constants.js';

// typed arrays over a shared buffer to aid binary conversion
const f64 = new float64Array(2);
const buf = f64.buffer;
const i64 = new int64Array(buf);
const u32 = new uint32Array(buf);
const i32 = new int32Array(buf);
const u8 = new uint8Array(buf);

/**
 * Return a value unchanged.
 * @template T
 * @param {T} value The value.
 * @returns {T} The value.
 */
export function identity(value) {
  return value;
}

/**
 * Return a value coerced to a BigInt.
 * @param {*} value The value.
 * @returns {bigint} The BigInt value.
 */
export function toBigInt(value) {
  return BigInt(value);
}

/**
 * Return an offset conversion method for the given data type.
 * @param {{ offsets: TypedArray}} type The array type.
 */
export function toOffset(type) {
  return isInt64ArrayType(type) ? toBigInt : identity;
}

/**
 * Return the number of days from a millisecond timestamp.
 * @param {number} value The millisecond timestamp.
 * @returns {number} The number of days.
 */
export function toDateDay(value) {
  return (value / 864e5) | 0;
}

/**
 * Return a timestamp conversion method for the given time unit.
 * @param {TimeUnit_} unit The time unit.
 * @returns {(value: number) => bigint} The conversion method.
 */
export function toTimestamp(unit) {
  return unit === TimeUnit.SECOND ? value => toBigInt(value / 1e3)
    : unit === TimeUnit.MILLISECOND ? toBigInt
    : unit === TimeUnit.MICROSECOND ? value => toBigInt(value * 1e3)
    : value => toBigInt(value * 1e6);
}

/**
 * Write month/day/nanosecond interval to a byte buffer.
 * @param {Array | Float64Array} interval The interval data.
 * @returns {Uint8Array} A byte buffer with the interval data.
 *  The returned buffer is reused across calls, and so should be
 *  copied to a target buffer immediately.
 */
export function toMonthDayNanoBytes([m, d, n]) {
  i32[0] = m;
  i32[1] = d;
  i64[1] = toBigInt(n);
  return u8;
}

/**
 * Coerce a bigint value to a number. Throws an error if the bigint value
 * lies outside the range of what a number can precisely represent.
 * @param {bigint} value The value to check and possibly convert.
 * @returns {number} The converted number value.
 */
export function toNumber(value) {
  if (value > Number.MAX_SAFE_INTEGER || value < Number.MIN_SAFE_INTEGER) {
    throw Error(`BigInt exceeds integer number representation: ${value}`);
  }
  return Number(value);
}

/**
 * Divide one BigInt value by another, and return the result as a number.
 * The division may involve unsafe integers and a loss of precision.
 * @param {bigint} num The numerator.
 * @param {bigint} div The divisor.
 * @returns {number} The result of the division as a floating point number.
 */
export function divide(num, div) {
  return Number(num / div) + Number(num % div) / Number(div);
}

/**
 * Return a 32-bit decimal conversion method for the given decimal scale.
 * @param {number} scale The scale mapping fractional digits to integers.
 * @returns {(value: number|bigint) => number} A conversion method that maps
 *  floating point numbers to 32-bit decimals.
 */
export function toDecimal32(scale) {
  return (value) => typeof value === 'bigint'
    ? Number(value)
    : Math.trunc(value * scale);
}

/**
 * Convert a floating point number or bigint to decimal bytes.
 * @param {number|bigint} value The number to encode. If a bigint, we assume
 *  it already represents the decimal in integer form with the correct scale.
 *  Otherwise, we assume a float that requires scaled integer conversion.
 * @param {BigUint64Array} buf The uint64 array to write to.
 * @param {number} offset The starting index offset into the array.
 * @param {number} stride The stride of an encoded decimal, in 64-bit steps.
 * @param {number} scale The scale mapping fractional digits to integers.
 */
export function toDecimal(value, buf, offset, stride, scale) {
  const v = typeof value === 'bigint'
    ? value
    : toBigInt(Math.trunc(value * scale));
  // assignment into uint64array performs needed truncation for us
  buf[offset] = v;
  if (stride > 1) {
    buf[offset + 1] = (v >> 64n);
    if (stride > 2) {
      buf[offset + 2] = (v >> 128n);
      buf[offset + 3] = (v >> 192n);
    }
  }
}

// helper method to extract uint64 values from bigints
const asUint64 = v => BigInt.asUintN(64, v);

/**
 * Convert a 64-bit decimal value to a bigint.
 * @param {BigUint64Array} buf The uint64 array containing the decimal bytes.
 * @param {number} offset The starting index offset into the array.
 * @returns {bigint} The converted decimal as a bigint, such that all
 *  fractional digits are scaled up to integers (for example, 1.12 -> 112).
 */
export function fromDecimal64(buf, offset) {
  return BigInt.asIntN(64, buf[offset]);
}

/**
 * Convert a 128-bit decimal value to a bigint.
 * @param {BigUint64Array} buf The uint64 array containing the decimal bytes.
 * @param {number} offset The starting index offset into the array.
 * @returns {bigint} The converted decimal as a bigint, such that all
 *  fractional digits are scaled up to integers (for example, 1.12 -> 112).
 */
export function fromDecimal128(buf, offset) {
  const i = offset << 1;
  let x;
  if (BigInt.asIntN(64, buf[i + 1]) < 0) {
    x = asUint64(~buf[i]) | (asUint64(~buf[i + 1]) << 64n);
    x = -(x + 1n);
  } else {
    x = buf[i] | (buf[i + 1] << 64n);
  }
  return x;
}

/**
 * Convert a 256-bit decimal value to a bigint.
 * @param {BigUint64Array} buf The uint64 array containing the decimal bytes.
 * @param {number} offset The starting index offset into the array.
 * @returns {bigint} The converted decimal as a bigint, such that all
 *  fractional digits are scaled up to integers (for example, 1.12 -> 112).
 */
export function fromDecimal256(buf, offset) {
  const i = offset << 2;
  let x;
  if (BigInt.asIntN(64, buf[i + 3]) < 0) {
    x = asUint64(~buf[i])
      | (asUint64(~buf[i + 1]) << 64n)
      | (asUint64(~buf[i + 2]) << 128n)
      | (asUint64(~buf[i + 3]) << 192n);
    x = -(x + 1n);
  } else {
    x = buf[i]
      | (buf[i + 1] << 64n)
      | (buf[i + 2] << 128n)
      | (buf[i + 3] << 192n);
  }
  return x;
}

/**
 * Convert a 16-bit float from integer bytes to a number.
 * Adapted from https://github.com/apache/arrow/blob/main/js/src/util/math.ts
 * @param {number} value The float as a 16-bit integer.
 * @returns {number} The converted 64-bit floating point number.
 */
export function fromFloat16(value) {
  const expo = (value & 0x7C00) >> 10;
  const sigf = (value & 0x03FF) / 1024;
  const sign = (-1) ** ((value & 0x8000) >> 15);
  switch (expo) {
    case 0x1F: return sign * (sigf ? Number.NaN : 1 / 0);
    case 0x00: return sign * (sigf ? 6.103515625e-5 * sigf : 0);
  }
  return sign * (2 ** (expo - 15)) * (1 + sigf);
}

/**
 * Convert a number to a 16-bit float as integer bytes..
 * Inspired by numpy's `npy_double_to_half`:
 * https://github.com/numpy/numpy/blob/5a5987291dc95376bb098be8d8e5391e89e77a2c/numpy/core/src/npymath/halffloat.c#L43
 * Adapted from https://github.com/apache/arrow/blob/main/js/src/util/math.ts
 * @param {number} value The 64-bit floating point number to convert.
 * @returns {number} The converted 16-bit integer.
 */
export function toFloat16(value) {
  if (value !== value) return 0x7E00; // NaN
  f64[0] = value;

  // Magic numbers:
  // 0x80000000 = 10000000 00000000 00000000 00000000 -- masks the 32nd bit
  // 0x7ff00000 = 01111111 11110000 00000000 00000000 -- masks the 21st-31st bits
  // 0x000fffff = 00000000 00001111 11111111 11111111 -- masks the 1st-20th bit
  const sign = (u32[1] & 0x80000000) >> 16 & 0xFFFF;
  let expo = (u32[1] & 0x7FF00000), sigf = 0x0000;

  if (expo >= 0x40F00000) {
    //
    // If exponent overflowed, the float16 is either NaN or Infinity.
    // Rules to propagate the sign bit: mantissa > 0 ? NaN : +/-Infinity
    //
    // Magic numbers:
    // 0x40F00000 = 01000000 11110000 00000000 00000000 -- 6-bit exponent overflow
    // 0x7C000000 = 01111100 00000000 00000000 00000000 -- masks the 27th-31st bits
    //
    // returns:
    // qNaN, aka 32256 decimal, 0x7E00 hex, or 01111110 00000000 binary
    // sNaN, aka 32000 decimal, 0x7D00 hex, or 01111101 00000000 binary
    // +inf, aka 31744 decimal, 0x7C00 hex, or 01111100 00000000 binary
    // -inf, aka 64512 decimal, 0xFC00 hex, or 11111100 00000000 binary
    //
    // If mantissa is greater than 23 bits, set to +Infinity like numpy
    if (u32[0] > 0) {
      expo = 0x7C00;
    } else {
      expo = (expo & 0x7C000000) >> 16;
      sigf = (u32[1] & 0x000FFFFF) >> 10;
    }
  } else if (expo <= 0x3F000000) {
    //
    // If exponent underflowed, the float is either signed zero or subnormal.
    //
    // Magic numbers:
    // 0x3F000000 = 00111111 00000000 00000000 00000000 -- 6-bit exponent underflow
    //
    sigf = 0x100000 + (u32[1] & 0x000FFFFF);
    sigf = 0x100000 + (sigf << ((expo >> 20) - 998)) >> 21;
    expo = 0;
  } else {
    //
    // No overflow or underflow, rebase the exponent and round the mantissa
    // Magic numbers:
    // 0x200 = 00000010 00000000 -- masks off the 10th bit
    //
    // Ensure the first mantissa bit (the 10th one) is 1 and round
    expo = (expo - 0x3F000000) >> 10;
    sigf = ((u32[1] & 0x000FFFFF) + 0x200) >> 10;
  }
  return sign | expo | sigf & 0xFFFF;
}
