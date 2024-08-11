/** Arrow version. */
export enum Version {
  V1 = 0,
  V2 = 1,
  V3 = 2,
  V4 = 3,
  V5 = 4
}

/** Endianness of Arrow-encoded data. */
export enum Endianness {
  Little = 0,
  Big = 1
}

/** Message header type codes. */
export enum MessageHeader {
  NONE = 0,
  Schema = 1,
  DictionaryBatch = 2,
  RecordBatch = 3,
  Tensor = 4,
  SparseTensor = 5
}

/** Floating point number precision. */
export enum Precision {
  HALF = 0,
  SINGLE = 1,
  DOUBLE = 2
};

/** Date units. */
export enum DateUnit {
  DAY = 0,
  MILLISECOND = 1
};

/** Time units. */
export enum TimeUnit {
  SECOND = 0,
  MILLISECOND = 1,
  MICROSECOND = 2,
  NANOSECOND = 3
};

/** Date/time interval units. */
export enum IntervalUnit {
  YEAR_MONTH = 0,
  DAY_TIME = 1,
  MONTH_DAY_NANO = 2
};

/** Union type modes. */
export enum UnionMode {
  Sparse = 0,
  Dense = 1
};

/** Field data type ids. */
export enum TypeId {
  Dictionary = -1,
  NONE = 0,
  Null = 1,
  Int = 2,
  Float = 3,
  Binary = 4,
  Utf8 = 5,
  Bool = 6,
  Decimal = 7,
  Date = 8,
  Time = 9,
  Timestamp = 10,
  Interval = 11,
  List = 12,
  Struct = 13,
  Union = 14,
  FixedSizeBinary = 15,
  FixedSizeList = 16,
  Map = 17,
  Duration = 18,
  LargeBinary = 19,
  LargeUtf8 = 20,
  LargeList = 21,
  RunEndEncoded = 22,
  BinaryView = 23,
  Utf8View = 24,
  ListView = 25,
  LargeListView = 26
}

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
export type ColumnArray<T> = T[] | TypedArray;

/** Custom metadata. */
export type Metadata = Map<string, string>;

/**
 * Arrow table schema.
 */
export interface Schema {
  version: Version;
  endianness: Endianness;
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

/** None data type. */
export type NoneType = { typeId: TypeId.NONE };

/** Null data type. */
export type NullType = { typeId: TypeId.Null };

/** Integer data type. */
export type IntType = { typeId: TypeId.Int, bitWidth: 8 | 16 | 32 | 64, signed: boolean, values: IntArrayConstructor };

/** Floating point number data type. */
export type FloatType = { typeId: TypeId.Float, precision: Precision, values: FloatArrayConstructor };

/** Opaque binary data type. */
export type BinaryType = { typeId: TypeId.Binary, offsets: Int32ArrayConstructor };

/** UTF-8 encoded string data type. */
export type Utf8Type = { typeId: TypeId.Utf8, offsets: Int32ArrayConstructor };

/** Boolean data type. */
export type BoolType = { typeId: TypeId.Bool };

/** Fixed decimal number data type. */
export type DecimalType = { typeId: TypeId.Decimal, precision: number, scale: number, bitWidth: 128 | 256, values: Uint32ArrayConstructor };

/** Date data type. */
export type DateType = { typeId: TypeId.Date, unit: DateUnit, values: DateTimeArrayConstructor };

/** Time data type. */
export type TimeType = { typeId: TypeId.Time, unit: TimeUnit, bitWidth: 32 | 64, signed: boolean, values: DateTimeArrayConstructor };

/** Timestamp data type. */
export type TimestampType = { typeId: TypeId.Timestamp, unit: TimeUnit, timezone: string | null, values: BigInt64ArrayConstructor };

/** Date/time interval data type. */
export type IntervalType = { typeId: TypeId.Interval, unit: IntervalUnit };

/** List data type. */
export type ListType = { typeId: TypeId.List, children: [DataType], offsets: Int32ArrayConstructor };

/** Struct data type. */
export type StructType = { typeId: TypeId.Struct, children: DataType[] };

/** Union data type. */
export type UnionType = { typeId: TypeId.Union, mode: UnionMode, typeIds: Int32Array, children: DataType[], offsets: Int32ArrayConstructor };

/** Fixed-size opaque binary data type. */
export type FixedSizeBinaryType = { typeId: TypeId.FixedSizeBinary, stride: number };

/** Fixed-size list data type. */
export type FixedSizeListType = { typeId: TypeId.FixedSizeList, stride: number, children: DataType[] };

/** Key-value map data type. */
export type MapType = { typeId: TypeId.Map, keysSorted: boolean, children: [DataType, DataType], offsets: Int32ArrayConstructor };

/** Duration data type. */
export type DurationType = { typeId: TypeId.Duration, unit: TimeUnit, values: BigInt64ArrayConstructor };

/** Opaque binary data type with 64-bit integer offsets for larger data. */
export type LargeBinaryType = { typeId: TypeId.LargeBinary, offsets: BigInt64ArrayConstructor };

/** UTF-8 encoded string data type with 64-bit integer offsets for larger data. */
export type LargeUtf8Type = { typeId: TypeId.LargeUtf8, offsets: BigInt64ArrayConstructor };

/** List data type with 64-bit integer offsets for larger data. */
export type LargeListType = { typeId: TypeId.LargeList, children: [DataType], offsets: BigInt64ArrayConstructor };

/** Dictionary-encoded data type. */
export type DictionaryType = { typeId: TypeId.Dictionary, type: DataType, id: number, keys: IntType, ordered: boolean };

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
  version: number;
  /** The message header type. */
  type: number;
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
