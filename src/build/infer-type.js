/**
 * @import { DataType, IntType, UnionType } from '../types.js'
 */
import { bool, dateDay, dictionary, field, fixedSizeList, float64, int16, int32, int64, int8, list, nullType, struct, timestamp, utf8 } from '../data-types.js';
import { isArray } from '../util/arrays.js';

/**
 * Infer the data type for a given input array.
 * @param {(visitor: (value: any) => void) => void} visit
 *  A function that applies a callback to successive data values.
 * @returns {DataType} The data type.
 */
export function inferType(visit) {
  const profile = profiler();
  visit(value => profile.add(value));
  return profile.type();
}

function profiler() {
  let length = 0;
  let nullCount = 0;
  let boolCount = 0;
  let numberCount = 0;
  let intCount = 0;
  let bigintCount = 0;
  let dateCount = 0;
  let dayCount = 0;
  let stringCount = 0;
  let arrayCount = 0;
  let structCount = 0;
  let min = Infinity;
  let max = -Infinity;
  let minLength = Infinity;
  let maxLength = -Infinity;
  let minBigInt;
  let maxBigInt;
  let arrayProfile;
  let structProfiles = {};

  return {
    add(value) {
      length++;
      if (value == null) {
        nullCount++;
        return;
      }
      switch (typeof value) {
        case 'string':
          stringCount++;
          break;
        case 'number':
          numberCount++;
          if (value < min) min = value;
          if (value > max) max = value;
          if (Number.isInteger(value)) intCount++;
          break;
        case 'bigint':
          bigintCount++;
          if (minBigInt === undefined) {
            minBigInt = maxBigInt = value;
          } else {
            if (value < minBigInt) minBigInt = value;
            if (value > maxBigInt) maxBigInt = value;
          }
          break;
        case 'boolean':
          boolCount++;
          break;
        case 'object':
          if (value instanceof Date) {
            dateCount++;
            // 1 day = 1000ms * 60s * 60min * 24hr = 86400000
            if ((+value % 864e5) === 0) dayCount++;
          } else if (isArray(value)) {
            arrayCount++;
            const len = value.length;
            if (len < minLength) minLength = len;
            if (len > maxLength) maxLength = len;
            arrayProfile ??= profiler();
            value.forEach(arrayProfile.add);
          } else {
            structCount++;
            for (const key in value) {
              const fieldProfiler = structProfiles[key]
                ?? (structProfiles[key] = profiler());
              fieldProfiler.add(value[key]);
            }
          }
      }
    },
    type() {
      const valid = length - nullCount;
      return valid === 0 ? nullType()
        : intCount === valid ? intType(min, max)
        : numberCount === valid ? float64()
        : bigintCount === valid ? bigintType(minBigInt, maxBigInt)
        : boolCount === valid ? bool()
        : dayCount === valid ? dateDay()
        : dateCount === valid ? timestamp()
        : stringCount === valid ? dictionary(utf8())
        : arrayCount === valid ? arrayType(arrayProfile.type(), minLength, maxLength)
        : structCount === valid ? struct(
            Object.entries(structProfiles).map(_ => field(_[0], _[1].type()))
          )
        : unionType();
    }
  };
}

/**
 * Return a list or fixed list type.
 * @param {DataType} type The child data type.
 * @param {number} minLength The minumum list length.
 * @param {number} maxLength The maximum list length.
 * @returns {DataType} The data type.
 */
function arrayType(type, minLength, maxLength) {
  return maxLength === minLength
    ? fixedSizeList(type, minLength)
    : list(type);
}

/**
 * @param {number} min
 * @param {number} max
 * @returns {DataType}
 */
function intType(min, max) {
  const v = Math.max(Math.abs(min) - 1, max);
  return v < (1 << 7) ? int8()
    : v < (1 << 15) ? int16()
    : v < (2 ** 31) ? int32()
    : float64();
}

/**
 * @param {bigint} min
 * @param {bigint} max
 * @returns {IntType}
 */
function bigintType(min, max) {
  const v = -min > max ? -min - 1n : max;
  if (v >= 2 ** 63) {
    throw new Error(`BigInt exceeds 64 bits: ${v}`);
  }
  return int64();
}

/**
 * @returns {UnionType}
 */
function unionType() {
  throw new Error('Mixed types detected, please define a union type.');
}
