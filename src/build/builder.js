/**
 * @import { DataType, ExtractionOptions } from '../types.js'
 * @import { BatchBuilder } from './builders/batch.js'
 */
import { batchType } from '../batch-type.js';
import { IntervalUnit, Type } from '../constants.js';
import { invalidDataType } from '../data-types.js';
import { isInt64ArrayType } from '../util/arrays.js';
import { toBigInt, toDateDay, toDecimal32, toFloat16, toTimestamp } from '../util/numbers.js';
import { BinaryBuilder } from './builders/binary.js';
import { BoolBuilder } from './builders/bool.js';
import { DecimalBuilder } from './builders/decimal.js';
import { DictionaryBuilder, dictionaryContext } from './builders/dictionary.js';
import { FixedSizeBinaryBuilder } from './builders/fixed-size-binary.js';
import { FixedSizeListBuilder } from './builders/fixed-size-list.js';
import { IntervalDayTimeBuilder, IntervalMonthDayNanoBuilder } from './builders/interval.js';
import { ListBuilder } from './builders/list.js';
import { MapBuilder } from './builders/map.js';
import { RunEndEncodedBuilder } from './builders/run-end-encoded.js';
import { StructBuilder } from './builders/struct.js';
import { DenseUnionBuilder, SparseUnionBuilder } from './builders/union.js';
import { Utf8Builder } from './builders/utf8.js';
import { DirectBuilder, Int64Builder, TransformBuilder } from './builders/values.js';

/**
 * Create a context object for shared builder state.
 * @param {ExtractionOptions} [options]  Batch extraction options.
* @param {ReturnType<dictionaryContext>} [dictionaries]
 *  Context object for tracking dictionaries.
 */
export function builderContext(
  options = {},
  dictionaries = dictionaryContext()
) {
  return {
    batchType: type => batchType(type, options),
    builder(type) { return builder(type, this); },
    dictionary(type) { return dictionaries.get(type, this); },
    finish: () => dictionaries.finish(options)
  };
}

/**
 * Returns a batch builder for the given type and builder context.
 * @param {DataType} type A data type.
 * @param {ReturnType<builderContext>} [ctx] A builder context.
 * @returns {BatchBuilder}
 */
export function builder(type, ctx = builderContext()) {
  const { typeId } = type;
  switch (typeId) {
    case Type.Int:
    case Type.Time:
    case Type.Duration:
      return isInt64ArrayType(type.values)
        ? new Int64Builder(type, ctx)
        : new DirectBuilder(type, ctx);
    case Type.Float:
      return type.precision
        ? new DirectBuilder(type, ctx)
        : new TransformBuilder(type, ctx, toFloat16)
    case Type.Binary:
    case Type.LargeBinary:
      return new BinaryBuilder(type, ctx);
    case Type.Utf8:
    case Type.LargeUtf8:
      return new Utf8Builder(type, ctx);
    case Type.Bool:
      return new BoolBuilder(type, ctx);
    case Type.Decimal:
      return type.bitWidth === 32
        ? new TransformBuilder(type, ctx, toDecimal32(type.scale))
        : new DecimalBuilder(type, ctx);
    case Type.Date:
      return new TransformBuilder(type, ctx, type.unit ? toBigInt : toDateDay);
    case Type.Timestamp:
      return new TransformBuilder(type, ctx, toTimestamp(type.unit));
    case Type.Interval:
      switch (type.unit) {
        case IntervalUnit.DAY_TIME:
          return new IntervalDayTimeBuilder(type, ctx);
        case IntervalUnit.MONTH_DAY_NANO:
          return new IntervalMonthDayNanoBuilder(type, ctx);
      }
      // case IntervalUnit.YEAR_MONTH:
      return new DirectBuilder(type, ctx);
    case Type.List:
    case Type.LargeList:
      return new ListBuilder(type, ctx);
    case Type.Struct:
      return new StructBuilder(type, ctx);
    case Type.Union:
      return type.mode
        ? new DenseUnionBuilder(type, ctx)
        : new SparseUnionBuilder(type, ctx);
    case Type.FixedSizeBinary:
      return new FixedSizeBinaryBuilder(type, ctx);
    case Type.FixedSizeList:
      return new FixedSizeListBuilder(type, ctx);
    case Type.Map:
      return new MapBuilder(type, ctx);
    case Type.RunEndEncoded:
      return new RunEndEncodedBuilder(type, ctx);

    case Type.Dictionary:
      return new DictionaryBuilder(type, ctx);
  }
  // case Type.BinaryView:
  // case Type.Utf8View:
  // case Type.ListView:
  // case Type.LargeListView:
  throw new Error(invalidDataType(typeId));
}
