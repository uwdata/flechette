/**
 * @import { DictionaryType, Endianness_, Field, IntType, Schema, Version_ } from '../types.js'
 */
import { Type } from '../constants.js';
import { dictionary, int32 } from '../data-types.js';
import { readBoolean, readInt16, readInt64, readObject, readOffset, readString, readUint8, readVector } from '../util/read.js';
import { decodeDataType } from './data-type.js';
import { decodeMetadata } from './metadata.js';

/**
 * Decode a table schema describing the fields and their data types.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @param {Version_} version Arrow version value
 * @returns {Schema} The schema
 */
export function decodeSchema(buf, index, version) {
  //  4: endianness (int16)
  //  6: fields (vector)
  //  8: metadata (vector)
  // 10: features (int64[])
  const get = readObject(buf, index);
  return {
    version,
    endianness: /** @type {Endianness_} */ (get(4, readInt16, 0)),
    fields: get(6, decodeSchemaFields, []),
    metadata: get(8, decodeMetadata)
  };
}

/**
 * @returns {Field[] | null}
 */
function decodeSchemaFields(buf, fieldsOffset) {
  return readVector(buf, fieldsOffset, 4, decodeField);
}

/**
 * @returns {Field}
 */
function decodeField(buf, index) {
  //  4: name (string)
  //  6: nullable (bool)
  //  8: type id (uint8)
  // 10: type (union)
  // 12: dictionary (table)
  // 14: children (vector)
  // 16: metadata (vector)
  const get = readObject(buf, index);
  const typeId = get(8, readUint8, Type.NONE);
  const typeOffset = get(10, readOffset, 0);
  const dict = get(12, decodeDictionary);
  const children = get(14, (buf, off) => decodeFieldChildren(buf, off));

  let type = decodeDataType(buf, typeOffset, typeId, children);
  if (dict) {
    dict.dictionary = type;
    type = dict;
  }

  return {
    name: get(4, readString),
    type,
    nullable: get(6, readBoolean, false),
    metadata: get(16, decodeMetadata)
  };
}

/**
 * @returns {Field[] | null}
 */
function decodeFieldChildren(buf, fieldOffset) {
  const children = readVector(buf, fieldOffset, 4, decodeField);
  return children.length ? children : null;
}

/**
 * @param {Uint8Array} buf
 * @param {number} index
 * @returns {DictionaryType}
 */
function decodeDictionary(buf, index) {
  if (!index) return null;
  //  4: id (int64)
  //  6: indexType (Int type)
  //  8: isOrdered (boolean)
  // 10: kind (int16) currently only dense array is supported
  const get = readObject(buf, index);
  return dictionary(
    null, // data type will be populated by caller
    get(6, decodeInt, int32()), // index type
    get(8, readBoolean, false), // ordered
    get(4, readInt64, 0), // id
  );
}

/**
 * Decode an integer data type.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data.
 * @param {number} index The starting index in the byte buffer.
 * @returns {IntType}
 */
function decodeInt(buf, index) {
  return /** @type {IntType} */ (
    decodeDataType(buf, index, Type.Int)
  );
}
