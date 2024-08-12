import {
  Version,
  Endianness,
  MessageHeader,
  Precision,
  DateUnit,
  TimeUnit,
  IntervalUnit,
  UnionMode
} from './constants.js';

// additional jsdoc types to export
export { Batch } from './batch.js';
export { Column } from './column.js';
export { Table } from './table.js';

/** A valid Arrow version number. */
export type Version_ = typeof Version[keyof typeof Version];

/** A valid endianness value. */
export type Endianness_ = typeof Endianness[keyof typeof Endianness];

/** A valid message header type value. */
export type MessageHeader_ = typeof MessageHeader[keyof typeof MessageHeader];

/** A valid floating point precision value. */
export type Precision_ = typeof Precision[keyof typeof Precision];

/** A valid date unit value. */
export type DateUnit_ = typeof DateUnit[keyof typeof DateUnit];

/** A valid time unit value. */
export type TimeUnit_ = typeof TimeUnit[keyof typeof TimeUnit];

/** A valid date/time interval unit value. */
export type IntervalUnit_ = typeof IntervalUnit[keyof typeof IntervalUnit];

/** A valid union type mode value. */
export type UnionMode_ = typeof UnionMode[keyof typeof UnionMode];

export type TypedArray =
  | Uint8Array
  | Uint16Array
  | Uint32Array
  | Int8Array
  | Int16Array
  | Int32Array
  | BigUint64Array
  | BigInt64Array
  | Float32Array
  | Float64Array;

export type OffsetArray =
  | Int32Array
  | BigInt64Array;

export type IntArrayConstructor =
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor
  | BigUint64ArrayConstructor
  | Int8ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | BigInt64ArrayConstructor;

export type FloatArrayConstructor =
  | Uint16ArrayConstructor
  | Float32ArrayConstructor
  | Float64ArrayConstructor;

export type DateTimeArrayConstructor =
  | Int32ArrayConstructor
  | BigInt64ArrayConstructor;

export type TypedArrayConstructor =
  | Uint8ArrayConstructor
  | Uint16ArrayConstructor
  | Uint32ArrayConstructor
  | BigUint64ArrayConstructor
  | Int8ArrayConstructor
  | Int16ArrayConstructor
  | Int32ArrayConstructor
  | BigInt64ArrayConstructor
  | Float32ArrayConstructor
  | Float64ArrayConstructor;

/** An extracted array of column values. */
export interface ValueArray<T> extends ArrayLike<T>, Iterable<T> {
  slice(start?: number, end?: number): ValueArray<T>;
}

/** Custom metadata. */
export type Metadata = Map<string, string>;

/**
 * Arrow table schema.
 */
export interface Schema {
  version: Version_;
  endianness: Endianness_;
  fields: Field[];
  metadata?: Metadata | null;
  dictionaryTypes: Map<number, DataType>;
}

/**
 * Arrow schema field definition.
 */
export interface Field {
  name: string,
  type: DataType;
  nullable: boolean;
  metadata: Metadata;
}

/** Valid integer bit widths. */
export type IntBitWidth = 8 | 16 | 32 | 64;

/** Dictionary-encoded data type. */
export type DictionaryType = { typeId: -1, type: DataType, id: number, keys: IntType, ordered: boolean };

/** None data type. */
export type NoneType = { typeId: 0 };

/** Null data type. */
export type NullType = { typeId: 1 };

/** Integer data type. */
export type IntType = { typeId: 2, bitWidth: IntBitWidth, signed: boolean, values: IntArrayConstructor };

/** Floating point number data type. */
export type FloatType = { typeId: 3, precision: Precision_, values: FloatArrayConstructor };

/** Opaque binary data type. */
export type BinaryType = { typeId: 4, offsets: Int32ArrayConstructor };

/** UTF-8 encoded string data type. */
export type Utf8Type = { typeId: 5, offsets: Int32ArrayConstructor };

/** Boolean data type. */
export type BoolType = { typeId: 6 };

/** Fixed decimal number data type. */
export type DecimalType = { typeId: 7, precision: number, scale: number, bitWidth: 128 | 256, values: Uint32ArrayConstructor };

/** Date data type. */
export type DateType = { typeId: 8, unit: DateUnit_, values: DateTimeArrayConstructor };

/** Time data type. */
export type TimeType = { typeId: 9, unit: TimeUnit_, bitWidth: 32 | 64, values: DateTimeArrayConstructor };

/** Timestamp data type. */
export type TimestampType = { typeId: 10, unit: TimeUnit_, timezone: string | null, values: BigInt64ArrayConstructor };

/** Date/time interval data type. */
export type IntervalType = { typeId: 11, unit: IntervalUnit_, values?: Int32ArrayConstructor };

/** List data type. */
export type ListType = { typeId: 12, children: [Field], offsets: Int32ArrayConstructor };

/** Struct data type. */
export type StructType = { typeId: 13, children: Field[] };

/** Union data type. */
export type UnionType = { typeId: 14, mode: UnionMode_, typeIds: Int32Array, children: Field[], offsets: Int32ArrayConstructor };

/** Fixed-size opaque binary data type. */
export type FixedSizeBinaryType = { typeId: 15, stride: number };

/** Fixed-size list data type. */
export type FixedSizeListType = { typeId: 16, stride: number, children: Field[] };

/** Key-value map data type. */
export type MapType = { typeId: 17, keysSorted: boolean, children: [Field, Field], offsets: Int32ArrayConstructor };

/** Duration data type. */
export type DurationType = { typeId: 18, unit: TimeUnit_, values: BigInt64ArrayConstructor };

/** Opaque binary data type with 64-bit integer offsets for larger data. */
export type LargeBinaryType = { typeId: 19, offsets: BigInt64ArrayConstructor };

/** UTF-8 encoded string data type with 64-bit integer offsets for larger data. */
export type LargeUtf8Type = { typeId: 20, offsets: BigInt64ArrayConstructor };

/** List data type with 64-bit integer offsets for larger data. */
export type LargeListType = { typeId: 21, children: [Field], offsets: BigInt64ArrayConstructor };

/**
 * Arrow field data types.
 */
export type DataType =
  | NoneType
  | NullType
  | IntType
  | FloatType
  | BinaryType
  | Utf8Type
  | BoolType
  | DecimalType
  | DateType
  | TimeType
  | TimestampType
  | IntervalType
  | ListType
  | StructType
  | UnionType
  | FixedSizeBinaryType
  | FixedSizeListType
  | MapType
  | DurationType
  | LargeBinaryType
  | LargeUtf8Type
  | LargeListType
  | DictionaryType;

/**
 * Arrow IPC record batch message.
 */
export interface RecordBatch {
  length: number;
  nodes: {length: number, nullCount: number}[];
  buffers: {offset: number, length: number}[];
  body?: Uint8Array;
}

/**
 * Arrow IPC dictionary batch message.
 */
export interface DictionaryBatch {
  id: number;
  data: RecordBatch;
  isDelta: boolean;
  body?: Uint8Array;
}

/**
 * Parsed Arrow IPC data, prior to table construction.
 */
export interface ArrowData {
  schema: Schema;
  dictionaries: DictionaryBatch[] | null;
  records: RecordBatch[] | null;
  metadata: Metadata | null;
}

/**
 * Parsed Arrow message data.
 */
export interface Message {
  /** The Arrow version. */
  version: Version_;
  /** The message header type. */
  type: MessageHeader_;
  /** The buffer integer index after the message. */
  index: number;
  /** The message content. */
  content?: Schema | RecordBatch | DictionaryBatch;
}

/**
 * Options for controlling how values are transformed when extracted
 * from am Arrow binary representation.
 */
export interface ExtractionOptions {
  /**
   * If true, extract dates and timestamps as JavaScript `Date` objects.
   * Otherwise, return numerical timestamp values (default).
   */
  useDate?: boolean;
  /**
   * If true, extract 64-bit integers as JavaScript `BigInt` values.
   * Otherwise, coerce long integers to JavaScript number values (default).
   */
  useBigInt?: boolean;
  /**
   * If true, extract Arrow 'Map' values as JavaScript `Map` instances.
   * Otherwise, return an array of [key, value] pairs compatible with
   * both `Map` and `Object.fromEntries` (default).
   */
  useMap?: boolean;
}
