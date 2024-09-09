import { batchType } from '../batch-type.js';
import { IntervalUnit, Type } from '../constants.js';
import { invalidDataType } from '../data-types.js';
import { isInt64ArrayType } from '../util/arrays.js';
import { toBigInt, toDateDay, toFloat16, toTimestamp, toYearMonth } from '../util/numbers.js';
import { BinaryBuilder } from './builders/binary.js';
import { BoolBuilder } from './builders/bool.js';
import { DecimalBuilder } from './builders/decimal.js';
import { DictionaryBuilder, dictionaryValues } from './builders/dictionary.js';
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

export function builderContext(options, dictMap = new Map) {
  let dictId = -1;
  return {
    batchType(type) {
      return batchType(type, options);
    },
    dictionary(type, id) {
      let dict;
      if (id != null) {
        dict = dictMap.get(id);
      } else {
        while (dictMap.has(dictId + 1)) ++dictId;
        id = dictId;
      }
      if (!dict) {
        dictMap.set(id, dict = dictionaryValues(id, type, this));
      }
      return dict;
    },
    dictionaryTypes() {
      const map = new Map;
      for (const dict of dictMap.values()) {
        map.set(dict.id, dict.type);
      }
      return map;
    },
    finish() {
      for (const dict of dictMap.values()) {
        dict.finish(options);
      }
    }
  };
}

/**
 * Returns a batch builder for the given type and builder context.
 * @param {import('../types.js').DataType} type A data type.
 * @param {ReturnType<builderContext>} [ctx] A builder context.
 * @returns {import('./builders/batch.js').BatchBuilder}
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
      return new DecimalBuilder(type, ctx);
    case Type.Date:
      return new TransformBuilder(type, ctx, type.unit ? toBigInt : toDateDay);
    case Type.Timestamp:
      return new TransformBuilder(type, ctx, toTimestamp(type.unit));
    case Type.Interval:
      switch (type.unit) {
        case IntervalUnit.YEAR_MONTH:
          return new TransformBuilder(type, ctx, toYearMonth);
        case IntervalUnit.DAY_TIME:
          return new IntervalDayTimeBuilder(type, ctx);
        case IntervalUnit.MONTH_DAY_NANO:
          return new IntervalMonthDayNanoBuilder(type, ctx);
      }
      break;
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
