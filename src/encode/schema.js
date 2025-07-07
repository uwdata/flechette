/**
 * @import { DictionaryType, Field, Schema } from '../types.js';
 * @import { Builder } from './builder.js';
 */
import { Type } from '../constants.js';
import { encodeDataType } from './data-type.js';
import { encodeMetadata } from './metadata.js';

const isLittleEndian = new Uint16Array(new Uint8Array([1, 0]).buffer)[0] === 1;

/**
 * @param {Builder} builder
 * @param {Schema} schema
 * @returns {number}
 */
export function encodeSchema(builder, schema) {
  const { fields, metadata } = schema;
  const fieldOffsets = fields.map(f => encodeField(builder, f));
  const fieldsVectorOffset = builder.addOffsetVector(fieldOffsets);
  const metadataOffset = encodeMetadata(builder, metadata);
  return builder.addObject(4, b => {
    b.addInt16(0, +(!isLittleEndian), 0);
    b.addOffset(1, fieldsVectorOffset, 0);
    b.addOffset(2, metadataOffset, 0);
    // NOT SUPPORTED: 3, features
  });
}

/**
 * @param {Builder} builder
 * @param {Field} field
 * @returns {number}
 */
function encodeField(builder, field) {
  const { name, nullable, type, metadata } = field;
  let { typeId } = type;

  // encode field data type
  let typeOffset = 0;
  let dictionaryOffset = 0;
  if (typeId !== Type.Dictionary) {
    typeOffset = encodeDataType(builder, type);
  } else {
    const dict = /** @type {DictionaryType} */ (type).dictionary;
    typeId = dict.typeId;
    dictionaryOffset = encodeDataType(builder, type);
    typeOffset = encodeDataType(builder, dict);
  }

  // encode children, metadata, name, and field object
  // @ts-ignore
  const childOffsets = (type.children || []).map(f => encodeField(builder, f));
  const childrenVectorOffset = builder.addOffsetVector(childOffsets);
  const metadataOffset = encodeMetadata(builder, metadata);
  const nameOffset = builder.addString(name);
  return builder.addObject(7, b => {
    b.addOffset(0, nameOffset, 0);
    b.addInt8(1, +nullable, +false);
    b.addInt8(2, typeId, Type.NONE);
    b.addOffset(3, typeOffset, 0);
    b.addOffset(4, dictionaryOffset, 0);
    b.addOffset(5, childrenVectorOffset, 0);
    b.addOffset(6, metadataOffset, 0);
  });
}
