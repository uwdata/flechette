import { Type } from '../constants.js';
import { readBoolean, readInt16, readInt64AsNum, readOffset, readString, readUint8, readVector, table } from '../util.js';
import { decodeDataType, decodeInt, typeInt } from './data-type.js';
import { decodeMetadata } from './metadata.js';

/**
 * Decode a table schema describing the fields and their data types.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @param {import('../types.js').Version} version Arrow version value
 * @returns {import('../types.js').Schema} The schema
 */
export function decodeSchema(buf, index, version) {
  const dictionaryTypes = new Map;
  //  4: endianness (int16)
  //  6: fields (vector)
  //  8: metadata (vector)
  // 10: features (int64[])
  const get = table(buf, index);
  return {
    version,
    endianness: get(4, readInt16, 0),
    fields: get(6, (buf, off) => decodeSchemaFields(buf, off, dictionaryTypes), []),
    metadata: get(8, decodeMetadata),
    dictionaryTypes
  };
}

/**
 * @returns {import('../types.js').Field[] | null}
 */
function decodeSchemaFields(buf, fieldsOffset, dictionaryTypes) {
  const { length, base } = readVector(buf, fieldsOffset);
  const fields = [];
  for (let i = 0; i < length; ++i) {
    fields.push(decodeField(buf, base + i * 4, dictionaryTypes));
  }
  return fields;
}

/**
 * @returns {import('../types.js').Field}
 */
function decodeField(buf, index, dictionaryTypes) {
  //  4: name (string)
  //  6: nullable (bool)
  //  8: type id (uint8)
  // 10: type (union)
  // 12: dictionary (table)
  // 14: children (vector)
  // 16: metadata (vector)
  const get = table(buf, index);
  const typeId = get(8, readUint8, Type.NONE);
  const typeOffset = get(10, readOffset, 0);
  const dict = get(12, (buf, off) => decodeDictionary(buf, off));
  const children = get(14, decodeFieldChildren);

  let type;
  if (dict) {
    const { id } = dict;
    let dictType = dictionaryTypes.get(id);
    if (!dictType) {
      // if dictionary encoded and the first time we've seen this id, decode
      // the type and children fields and add to the dictionary map.
      dictType = decodeDataType(buf, typeOffset, typeId, children);
      dictionaryTypes.set(id, dictType);
    }
    dict.type = dictType;
    type = dict;
  } else {
    type = decodeDataType(buf, typeOffset, typeId, children);
  }

  return {
    name: get(4, readString),
    type,
    nullable: get(6, readBoolean, false),
    metadata: get(16, decodeMetadata)
  };
}

/**
 * @returns {import('../types.js').Field[] | null}
 */
function decodeFieldChildren(buf, fieldOffset, dictionaries) {
  const { length, base } = readVector(buf, fieldOffset);
  const children = [];
  for (let i = 0; i < length; ++i) {
    const pos = base + i * 4;
    children.push(decodeField(buf, pos, dictionaries));
  }
  return children.length ? children : null;
}

/**
 * @param {Uint8Array} buf
 * @param {number} index
 * @returns {import('../types.js').DictionaryType}
 */
function decodeDictionary(buf, index) {
  if (!index) return null;
  //  4: id (int64)
  //  6: indexType (Int type)
  //  8: isOrdered (boolean)
  // 10: kind (int16) currently only dense array is supported
  const get = table(buf, index);
  return {
    type: null, // to be populated by caller
    typeId: Type.Dictionary,
    id: get(4, readInt64AsNum, 0),
    keys: get(6, decodeInt, typeInt(32, true)), // index defaults to int32
    ordered: get(8, readBoolean, false)
  };
}
