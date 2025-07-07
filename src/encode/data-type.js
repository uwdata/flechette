/**
 * @import { DataType } from '../types.js';
 * @import { Builder } from './builder.js';
 */
import { DateUnit, IntervalUnit, Precision, TimeUnit, Type, UnionMode } from '../constants.js';
import { invalidDataType } from '../data-types.js';
import { checkOneOf } from '../util/objects.js';

/**
 * Encode a data type into a flatbuffer.
 * @param {Builder} builder
 * @param {DataType} type
 * @returns {number} The offset at which the data type is written.
 */
export function encodeDataType(builder, type) {
  const typeId = checkOneOf(type.typeId, Type, invalidDataType);

  switch (typeId) {
    case Type.Dictionary:
      return encodeDictionary(builder, type);
    case Type.Int:
      return encodeInt(builder, type);
    case Type.Float:
      return encodeFloat(builder, type);
    case Type.Decimal:
      return encodeDecimal(builder, type);
    case Type.Date:
      return encodeDate(builder, type);
    case Type.Time:
      return encodeTime(builder, type);
    case Type.Timestamp:
      return encodeTimestamp(builder, type);
    case Type.Interval:
      return encodeInterval(builder, type);
    case Type.Duration:
      return encodeDuration(builder, type);
    case Type.FixedSizeBinary:
    case Type.FixedSizeList:
      return encodeFixedSize(builder, type);
    case Type.Map:
      return encodeMap(builder, type);
    case Type.Union:
      return encodeUnion(builder, type);
  }
  // case Type.Null:
  // case Type.Binary:
  // case Type.LargeBinary:
  // case Type.BinaryView:
  // case Type.Bool:
  // case Type.Utf8:
  // case Type.Utf8View:
  // case Type.LargeUtf8:
  // case Type.List:
  // case Type.ListView:
  // case Type.LargeList:
  // case Type.LargeListView:
  // case Type.RunEndEncoded:
  // case Type.Struct:
  return builder.addObject(0);
}

function encodeDate(builder, type) {
  return builder.addObject(1, b => {
    b.addInt16(0, type.unit, DateUnit.MILLISECOND);
  });
}

function encodeDecimal(builder, type) {
  return builder.addObject(3, b => {
    b.addInt32(0, type.precision, 0);
    b.addInt32(1, type.scale, 0);
    b.addInt32(2, type.bitWidth, 128);
  });
}

function encodeDuration(builder, type) {
  return builder.addObject(1, b => {
    b.addInt16(0, type.unit, TimeUnit.MILLISECOND);
  });
}

function encodeFixedSize(builder, type) {
  return builder.addObject(1, b => {
    b.addInt32(0, type.stride, 0);
  });
}

function encodeFloat(builder, type) {
  return builder.addObject(1, b => {
    b.addInt16(0, type.precision, Precision.HALF);
  });
}

function encodeInt(builder, type) {
  return builder.addObject(2, b => {
    b.addInt32(0, type.bitWidth, 0);
    b.addInt8(1, +type.signed, 0);
  });
}

function encodeInterval(builder, type) {
  return builder.addObject(1, b => {
    b.addInt16(0, type.unit, IntervalUnit.YEAR_MONTH);
  });
}

function encodeMap(builder, type) {
  return builder.addObject(1, b => {
    b.addInt8(0, +type.keysSorted, 0);
  });
}

function encodeTime(builder, type) {
  return builder.addObject(2, b => {
    b.addInt16(0, type.unit, TimeUnit.MILLISECOND);
    b.addInt32(1, type.bitWidth, 32);
  });
}

function encodeTimestamp(builder, type) {
  const timezoneOffset = builder.addString(type.timezone);
  return builder.addObject(2, b => {
    b.addInt16(0, type.unit, TimeUnit.SECOND);
    b.addOffset(1, timezoneOffset, 0);
  });
}

function encodeUnion(builder, type) {
  const typeIdsOffset = builder.addVector(
    type.typeIds, 4, 4,
    (builder, value) => builder.addInt32(value)
  );
  return builder.addObject(2, b => {
    b.addInt16(0, type.mode, UnionMode.Sparse);
    b.addOffset(1, typeIdsOffset, 0);
  });
}

function encodeDictionary(builder, type) {
  // The Arrow spec uses signed 32-bit integers as the default index type.
  // However, multiple 3rd party tools fail on a null (default) index type,
  // so we always encode the index data type explicitly here.
  return builder.addObject(4, b => {
    b.addInt64(0, type.id, 0);
    b.addOffset(1, encodeDataType(builder, type.indices), 0);
    b.addInt8(2, +type.ordered, 0);
    // NOT SUPPORTED: 3, dictionaryKind (defaults to dense array)
  });
}
