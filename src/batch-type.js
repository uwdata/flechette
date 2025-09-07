/**
 * @import { DataType, ExtractionOptions } from './types.js';
 */
import { BinaryBatch, BinaryViewBatch, BoolBatch, DateBatch, DateDayBatch, DateDayMillisecondBatch, Decimal32NumberBatch, DecimalBigIntBatch, DecimalNumberBatch, DenseUnionBatch, DictionaryBatch, DirectBatch, FixedBinaryBatch, FixedListBatch, Float16Batch, Int64Batch, IntervalDayTimeBatch, IntervalMonthDayNanoBatch, LargeBinaryBatch, LargeListBatch, LargeListViewBatch, LargeUtf8Batch, ListBatch, ListViewBatch, MapBatch, MapEntryBatch, NullBatch, RunEndEncodedBatch, SparseUnionBatch, StructBatch, StructProxyBatch, TimestampMicrosecondBatch, TimestampMillisecondBatch, TimestampNanosecondBatch, TimestampSecondBatch, Utf8Batch, Utf8ViewBatch } from './batch.js';
import { DateUnit, IntervalUnit, TimeUnit, Type } from './constants.js';
import { invalidDataType } from './data-types.js';

/**
 * Return a batch constructor for the given data type and extraction options.
 * @param {DataType} type The data type.
 * @param {ExtractionOptions} options The extraction options.
 */
export function batchType(type, options = {}) {
  const { typeId, bitWidth, mode, precision, unit } = /** @type {any} */(type);
  const { useBigInt, useDate, useDecimalInt, useMap, useProxy } = options;

  switch (typeId) {
    case Type.Null: return NullBatch;
    case Type.Bool: return BoolBatch;
    case Type.Int:
    case Type.Time:
    case Type.Duration:
      return useBigInt || bitWidth < 64 ? DirectBatch : Int64Batch;
    case Type.Float:
      return precision ? DirectBatch : Float16Batch;
    case Type.Date:
      return wrap(
        unit === DateUnit.DAY ? DateDayBatch : DateDayMillisecondBatch,
        useDate && DateBatch
      );
    case Type.Timestamp:
      return wrap(
        unit === TimeUnit.SECOND ? TimestampSecondBatch
          : unit === TimeUnit.MILLISECOND ? TimestampMillisecondBatch
          : unit === TimeUnit.MICROSECOND ? TimestampMicrosecondBatch
          : TimestampNanosecondBatch,
        useDate && DateBatch
      );
    case Type.Decimal:
      return bitWidth === 32
        ? (useDecimalInt ? DirectBatch : Decimal32NumberBatch)
        : (useDecimalInt ? DecimalBigIntBatch : DecimalNumberBatch);
    case Type.Interval:
      return unit === IntervalUnit.DAY_TIME ? IntervalDayTimeBatch
        : unit === IntervalUnit.YEAR_MONTH ? DirectBatch
        : IntervalMonthDayNanoBatch;
    case Type.FixedSizeBinary: return FixedBinaryBatch;
    case Type.Utf8: return Utf8Batch;
    case Type.LargeUtf8: return LargeUtf8Batch;
    case Type.Binary: return BinaryBatch;
    case Type.LargeBinary: return LargeBinaryBatch;
    case Type.BinaryView: return BinaryViewBatch;
    case Type.Utf8View: return Utf8ViewBatch;
    case Type.List: return ListBatch;
    case Type.LargeList: return LargeListBatch;
    case Type.Map: return useMap ? MapBatch : MapEntryBatch;
    case Type.ListView: return ListViewBatch;
    case Type.LargeListView: return LargeListViewBatch;
    case Type.FixedSizeList: return FixedListBatch;
    case Type.Struct: return useProxy ? StructProxyBatch : StructBatch;
    case Type.RunEndEncoded: return RunEndEncodedBatch;
    case Type.Dictionary: return DictionaryBatch;
    case Type.Union: return mode ? DenseUnionBatch : SparseUnionBatch;
  }
  throw new Error(invalidDataType(typeId));
}

function wrap(BaseClass, WrapperClass) {
  return WrapperClass
    ? class WrapBatch extends WrapperClass {
        constructor(options) {
          super(new BaseClass(options));
        }
      }
    : BaseClass;
}
