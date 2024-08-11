import { arrayTypeInt, float32, float64, int32, int64, uint16, uint32 } from '../array-types.js';
import { DateUnit, IntervalUnit, Precision, TimeUnit, Type, UnionMode } from '../constants.js';
import { keyFor, readBoolean, readInt16, readInt32, readOffset, readString, readVector, table } from '../util.js';

/**
 * Decode a data type definition for a field.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data.
 * @param {number} index The starting index in the byte buffer.
 * @param {number} typeId The data type id.
 * @param {any[]} [children] A list of parsed child fields.
 * @returns {import('../types.js').DataType} The data type.
 */
export function decodeDataType(buf, index, typeId, children) {
  switch (typeId) {
    case Type.NONE:
    case Type.Null:
    case Type.Bool:
      return { typeId };
    case Type.Binary:
    case Type.Utf8:
      return { typeId, offsets: int32 };
    case Type.LargeBinary:
    case Type.LargeUtf8:
      return { typeId, offsets: int64 };
    case Type.List:
      return { typeId, children: [children?.[0] ?? null], offsets: int32 };
    case Type.LargeList:
      return { typeId, children: [children?.[0] ?? null], offsets: int64 };
    case Type.Struct:
      return { typeId, children };
    case Type.Int:
      return decodeInt(buf, index);
    case Type.Float:
      return decodeFloat(buf, index);
    case Type.Decimal:
      return decodeDecimal(buf, index);
    case Type.Date:
      return decodeDate(buf, index);
    case Type.Time:
      return decodeTime(buf, index);
    case Type.Timestamp:
      return decodeTimestamp(buf, index);
    case Type.Interval:
      return decodeInterval(buf, index);
    case Type.Duration:
      return decodeDuration(buf, index);
    case Type.FixedSizeBinary:
      return decodeFixedSizeBinary(buf, index);
    case Type.FixedSizeList:
      return decodeFixedSizeList(buf, index, children);
    case Type.Map:
      return decodeMap(buf, index, children);
    case Type.Union:
      return decodeUnion(buf, index, children);
  }
  // TODO: collect errors, skip failures?
  throw new Error(`Unrecognized type: "${keyFor(Type, typeId)}" (id ${typeId})`);
}

/**
 * Construct an integer data type.
 * @param {8|16|32|64} bitWidth The integer bit width.
 * @param {boolean} signed Flag for signed or unsigned integers.
 * @returns {import('../types.js').IntType} The integer data type.
 */
export function typeInt(bitWidth, signed) {
  return {
    typeId: Type.Int,
    bitWidth,
    signed,
    values: arrayTypeInt(bitWidth, signed)
  };
}

/**
 * Decode an integer data type from binary data.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data.
 * @param {number} index The starting index in the byte buffer.
 * @returns {import('../types.js').IntType} The integer data type.
 */
export function decodeInt(buf, index) {
  //  4: bitWidth
  //  6: isSigned
  const get = table(buf, index);
  return typeInt(
    // @ts-ignore
    get(4, readInt32, 0), // bitwidth
    get(6, readBoolean, false) // signed
  );
}

function decodeFloat(buf, index) {
  //  4: precision
  const get = table(buf, index);
  const precision = get(4, readInt16, Precision.HALF);
  return {
    typeId: Type.Float,
    precision,
    values: precision === Precision.HALF ? uint16
      : precision === Precision.SINGLE ? float32
      : precision === Precision.DOUBLE ? float64
      : null
  };
}

function decodeDecimal(buf, index) {
  //  4: precision
  //  6: scale
  //  8: bitWidth
  const get = table(buf, index);
  const bitWidth = get(8, readInt32, 128);
  return {
    typeId: Type.Decimal,
    precision: get(4, readInt32, 0),
    scale: get(6, readInt32, 0),
    bitWidth,
    values: uint32
  };
}

function decodeDate(buf, index) {
  //  4: unit
  const get = table(buf, index);
  const unit = get(4, readInt16, DateUnit.MILLISECOND);
  return {
    typeId: Type.Date,
    unit,
    values: unit === DateUnit.DAY ? int32 : int64
  };
}

function decodeTime(buf, index) {
  //  4: unit
  //  6: bitWidth
  const get = table(buf, index);
  const bitWidth = get(6, readInt32, 32);
  return {
    typeId: Type.Time,
    unit: get(4, readInt16, TimeUnit.MILLISECOND),
    bitWidth,
    values: bitWidth === 32 ? int32 : int64
  };
}

function decodeTimestamp(buf, index) {
  //  4: unit
  //  6: timezone
  const get = table(buf, index);
  return {
    typeId: Type.Timestamp,
    unit: get(4, readInt16, TimeUnit.SECOND),
    timezone: get(6, readString),
    values: int64
  };
}

function decodeInterval(buf, index) {
  //  4: unit
  const get = table(buf, index);
  const unit = get(4, readInt16, IntervalUnit.YEAR_MONTH);
  return {
    typeId: Type.Interval,
    unit,
    values: unit === IntervalUnit.MONTH_DAY_NANO ? undefined : int32
  };
}

function decodeDuration(buf, index) {
  //  4: unit
  const get = table(buf, index);
  return {
    typeId: Type.Duration,
    unit: get(4, readInt16, TimeUnit.MILLISECOND),
    values: int64
  };
}

function decodeFixedSizeBinary(buf, index) {
  //  4: size (byteWidth)
  const get = table(buf, index);
  return {
    typeId: Type.FixedSizeBinary,
    stride: get(4, readInt32, 0)
  };
}

function decodeFixedSizeList(buf, index, children) {
  //  4: size (listSize)
  const get = table(buf, index);
  return {
    typeId: Type.FixedSizeList,
    stride: get(4, readInt32, 0),
    children: [children?.[0] ?? null]
  };
}

function decodeMap(buf, index, children) {
  //  4: keysSorted (bool)
  const get = table(buf, index);
  return {
    typeId: Type.Map,
    keysSorted: get(4, readBoolean, false),
    children,
    offsets: int32
  };
}

function decodeUnion(buf, index, children) {
  //  4: mode
  //  6: typeIds
  const get = table(buf, index);
  const { length, base } = readVector(buf, get(6, readOffset));
  return {
    typeId: Type.Union,
    mode: get(4, readInt16, UnionMode.Sparse),
    typeIds: new int32(buf.buffer, buf.byteOffset + base, length),
    children: children ?? [],
    offsets: int32
  };
}
