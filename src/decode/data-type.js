/**
 * @import { DataType, Field } from '../types.js'
 */
import { DateUnit, IntervalUnit, Precision, TimeUnit, Type, UnionMode } from '../constants.js';
import { binary, date, decimal, duration, fixedSizeBinary, fixedSizeList, float, int, interval, invalidDataType, largeBinary, largeList, largeListView, largeUtf8, list, listView, mapType, runEndEncoded, struct, time, timestamp, union, utf8 } from '../data-types.js';
import { checkOneOf } from '../util/objects.js';
import { readBoolean, readInt16, readInt32, readObject, readOffset, readString, readVector } from '../util/read.js';

/**
 * Decode a data type definition for a field.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data.
 * @param {number} index The starting index in the byte buffer.
 * @param {number} typeId The data type id.
 * @param {Field[]} [children] A list of parsed child fields.
 * @returns {DataType} The data type.
 */
export function decodeDataType(buf, index, typeId, children) {
  checkOneOf(typeId, Type, invalidDataType);
  const get = readObject(buf, index);

  switch (typeId) {
    // types without flatbuffer objects
    case Type.Binary: return binary();
    case Type.Utf8: return utf8();
    case Type.LargeBinary: return largeBinary();
    case Type.LargeUtf8: return largeUtf8();
    case Type.List: return list(children[0]);
    case Type.ListView: return listView(children[0]);
    case Type.LargeList: return largeList(children[0]);
    case Type.LargeListView: return largeListView(children[0]);
    case Type.Struct: return struct(children);
    case Type.RunEndEncoded: return runEndEncoded(children[0], children[1]);

    // types with flatbuffer objects
    case Type.Int: return int(
      // @ts-ignore
      get(4, readInt32, 0), // bitwidth
      get(6, readBoolean, false) // signed
    );
    case Type.Float: return float(
      // @ts-ignore
      get(4, readInt16, Precision.HALF) // precision
    );
    case Type.Decimal: return decimal(
      get(4, readInt32, 0), // precision
      get(6, readInt32, 0), // scale
      // @ts-ignore
      get(8, readInt32, 128) // bitwidth
    );
    case Type.Date: return date(
      // @ts-ignore
      get(4, readInt16, DateUnit.MILLISECOND) // unit
    );
    case Type.Time: return time(
      // @ts-ignore
      get(4, readInt16, TimeUnit.MILLISECOND) // unit
    );
    case Type.Timestamp: return timestamp(
      // @ts-ignore
      get(4, readInt16, TimeUnit.SECOND), // unit
      get(6, readString) // timezone
    );
    case Type.Interval: return interval(
      // @ts-ignore
      get(4, readInt16, IntervalUnit.YEAR_MONTH) // unit
    );
    case Type.Duration: return duration(
      // @ts-ignore
      get(4, readInt16, TimeUnit.MILLISECOND) // unit
    );

    case Type.FixedSizeBinary: return fixedSizeBinary(
      get(4, readInt32, 0) // stride
    );
    case Type.FixedSizeList: return fixedSizeList(
      children[0],
      get(4, readInt32, 0), // stride
    );
    case Type.Map: return mapType(
      get(4, readBoolean, false), // keysSorted
      children[0]
    );

    case Type.Union: return union(
      // @ts-ignore
      get(4, readInt16, UnionMode.Sparse), // mode
      children,
      readVector(buf, get(6, readOffset), 4, readInt32) // type ids
    );
  }
  // case Type.NONE:
  // case Type.Null:
  // case Type.Bool:
  // case Type.BinaryView:
  // case Type.Utf8View:
  // @ts-ignore
  return { typeId };
}
