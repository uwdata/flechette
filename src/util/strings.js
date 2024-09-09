import { isArray } from './arrays.js';
import { isDate } from './objects.js';

const textDecoder = new TextDecoder('utf-8');
const textEncoder = new TextEncoder();

/**
 * Return a UTF-8 string decoded from a byte buffer.
 * @param {Uint8Array} buf The byte buffer.
 * @returns {string} The decoded string.
 */
export function decodeUtf8(buf) {
  return textDecoder.decode(buf);
}

/**
 * Return a byte buffer encoded from a UTF-8 string.
 * @param {string } str The string to encode.
 * @returns {Uint8Array} The encoded byte buffer.
 */
export function encodeUtf8(str) {
  return textEncoder.encode(str);
}

/**
 * Return a string-coercible key value that uniquely identifies a value.
 * @param {*} value The input value.
 * @returns {string} The key string.
 */
export function keyString(value) {
  const val = typeof value !== 'object' || !value ? (value ?? null)
    : isDate(value) ? +value
    // @ts-ignore
    : isArray(value) ? `[${value.map(keyString)}]`
    : objectKey(value);
  return `${val}`;
}

function objectKey(value) {
  let s = '';
  let i = -1;
  for (const k in value) {
    if (++i > 0) s += ',';
    s += `"${k}":${keyString(value[k])}`;
  }
  return `{${s}}`;
}
