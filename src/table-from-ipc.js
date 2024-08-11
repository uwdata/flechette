import { int8 } from './array-types.js';
import {
  BinaryBatch,
  BoolBatch, DateBatch, DateDayBatch, DateDayMillisecondBatch, DecimalBatch,
  DenseUnionBatch, DictionaryBatch, DirectBatch, FixedBatch, FixedListBatch,
  Float16Batch, Int64Batch, IntervalDayTimeBatch, IntervalMonthDayNanoBatch,
  IntervalYearMonthBatch, LargeBinaryBatch, LargeListBatch, LargeUtf8Batch,
  ListBatch, MapBatch, MapEntryBatch, NullBatch, SparseUnionBatch, StructBatch,
  TimestampMicrosecondBatch, TimestampMillisecondBatch, TimestampNanosecondBatch,
  TimestampSecondBatch, Utf8Batch
} from './batch.js';
import { columnBuilder } from './column.js';
import {
  DateUnit, IntervalUnit, Precision, TimeUnit, Type, UnionMode, Version
} from './constants.js';
import { parseIPC } from './parse-ipc.js';
import { Table } from './table.js';
import { keyFor } from './util.js';

/**
 * Decode [Apache Arrow IPC data][1] and return a new Table. The input binary
 * data may be either an `ArrayBuffer` or `Uint8Array`. For Arrow data in the
 * [IPC 'stream' format][2], an array of `Uint8Array` values is also supported.
 *
 * [1]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
 * [2]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 * @param {ArrayBuffer | Uint8Array | Uint8Array[]} data
 *  The source byte buffer, or an array of buffers. If an array, each byte
 *  array may contain one or more self-contained messages. Messages may NOT
 *  span multiple byte arrays.
 * @param {import('./types.js').ExtractionOptions} [options]
 *  Options for controlling how values are transformed when extracted
 *  from am Arrow binary representation.
 * @returns {Table} A Table instance.
 */
export function tableFromIPC(data, options) {
  return createTable(parseIPC(data), options);
}

/**
 * Create a table from parsed IPC data.
 * @param {import('./types.js').ArrowData} data
 *  The IPC data, as returned by parseIPC.
 * @param {import('./types.js').ExtractionOptions} [options]
 *  Options for controlling how values are transformed when extracted
 *  from am Arrow binary representation.
 * @returns {Table} A Table instance.
 */
export function createTable(data, options = {}) {
  const { schema, dictionaries, records } = data;
  const { version, fields, dictionaryTypes } = schema;
  const dictionaryMap = new Map;
  const context = contextGenerator(options, version, dictionaryMap);

  // decode dictionaries
  const dicts = new Map;
  for (const dict of dictionaries) {
    const { id, data, isDelta, body } = dict;
    const type = dictionaryTypes.get(id);
    const chunk = visit(type, context({ ...data, body }));
    if (!dicts.has(id)) {
      if (isDelta) {
        throw new Error('Delta update can not be first dictionary batch.');
      }
      dicts.set(id, columnBuilder(`${id}`, type).add(chunk));
    } else {
      const dict = dicts.get(id);
      if (!isDelta) dict.clear();
      dict.add(chunk);
    }
  }
  dicts.forEach((value, key) => dictionaryMap.set(key, value.done()));

  // decode column fields
  const cols = fields.map(({ name, type }) => columnBuilder(name, type));
  for (const batch of records) {
    const ctx = context(batch);
    cols.forEach(c => c.add(visit(c.type, ctx)));
  }

  return new Table(schema, cols.map(c => c.done()));
}

/**
 * Context object generator for field visitation and buffer definition.
 */
function contextGenerator(options, version, dictionaryMap) {
  const base = {
    version,
    options,
    dictionary: id => dictionaryMap.get(id),
  };

  // return a context generator
  return batch => {
    const { length, nodes, buffers, body } = batch;
    let nodeIndex = -1;
    let bufferIndex = -1;
    return {
      ...base,
      length,
      debug: () => ({nodeIndex, bufferIndex, nodes, buffers}),
      node: () => nodes[++nodeIndex],
      buffer: (ArrayType) => {
        const { length, offset } = buffers[++bufferIndex];
        return ArrayType
          ? new ArrayType(body.buffer, body.byteOffset + offset, length / ArrayType.BYTES_PER_ELEMENT)
          : body.subarray(offset, offset + length)
      },
      visitAll(list) { return list.map(x => visit(x.type, this)); }
    };
  };
}

/**
 * Visit a field, instantiating views of buffer regions.
 */
function visit(type, ctx) {
  const { typeId, bitWidth, precision, unit } = type;
  const { useBigInt, useDate, useMap } = ctx.options;

  // no field node, no buffers
  if (typeId === Type.Null) {
    const { length } = ctx;
    return new NullBatch({ type, length, nullCount: length });
  }

  // extract the next { length, nullCount } field node
  const node = ctx.node();
  const opt = { ...node, type };

  // batch constructors
  const value = (BatchType) => new BatchType({
    ...opt,
    validity: ctx.buffer(),
    values: ctx.buffer(type.values)
  });
  const offset = (BatchType) => new BatchType({
    ...opt,
    validity: ctx.buffer(),
    offsets: ctx.buffer(type.offsets),
    values: ctx.buffer()
  });
  const list = (BatchType) => new BatchType({
    ...opt,
    validity: ctx.buffer(),
    offsets: ctx.buffer(type.offsets),
    children: ctx.visitAll(type.children)
  });
  const kids = (BatchType) => new BatchType({
    ...opt,
    validity: ctx.buffer(),
    children: ctx.visitAll(type.children)
  });
  const date = useDate
    ? (BatchType) => new DateBatch(value(BatchType))
    : value;

  switch (typeId) {
    // validity and data value buffers
    case Type.Bool:
      return value(BoolBatch);
    case Type.Int:
    case Type.Time:
    case Type.Duration:
      return value(bitWidth === 64 && !useBigInt ? Int64Batch : DirectBatch);
    case Type.Float:
      return value(precision === Precision.HALF ? Float16Batch : DirectBatch);
    case Type.Date:
      return date(unit === DateUnit.DAY ? DateDayBatch : DateDayMillisecondBatch);
    case Type.Timestamp:
      return date(unit === TimeUnit.SECOND ? TimestampSecondBatch
        : unit === TimeUnit.MILLISECOND ? TimestampMillisecondBatch
        : unit === TimeUnit.MICROSECOND ? TimestampMicrosecondBatch
        : unit === TimeUnit.NANOSECOND ? TimestampNanosecondBatch
        : null);
    case Type.Decimal:
      return value(DecimalBatch);
    case Type.Interval:
      return value(unit === IntervalUnit.DAY_TIME ? IntervalDayTimeBatch
        : unit === IntervalUnit.YEAR_MONTH ? IntervalYearMonthBatch
        : unit === IntervalUnit.MONTH_DAY_NANO ? IntervalMonthDayNanoBatch
        : null);
    case Type.FixedSizeBinary:
      return value(FixedBatch);

    // validity, offset, and value buffers
    case Type.Utf8: return offset(Utf8Batch);
    case Type.LargeUtf8: return offset(LargeUtf8Batch);
    case Type.Binary: return offset(BinaryBatch);
    case Type.LargeBinary: return offset(LargeBinaryBatch);

    // validity, offset, and list child
    case Type.List: return list(ListBatch);
    case Type.LargeList: return list(LargeListBatch);
    case Type.Map: return list(useMap ? MapBatch : MapEntryBatch);

    // validity and children
    case Type.FixedSizeList: return kids(FixedListBatch);
    case Type.Struct: return kids(StructBatch);

    // dictionary
    case Type.Dictionary: {
      const { id, keys } = type;
      return new DictionaryBatch({
        ...opt,
        validity: ctx.buffer(),
        values: ctx.buffer(keys.values),
        dictionary: ctx.dictionary(id)
      });
    }

    // union
    case Type.Union: {
      if (ctx.version < Version.V5) {
        ctx.buffer(); // skip unused null bitmap
      }
      const isSparse = type.mode === UnionMode.Sparse;
      const typeIds = ctx.buffer(int8);
      const offsets = isSparse ? null : ctx.buffer(type.offsets);
      const children = ctx.visitAll(type.children);
      const options = { ...opt, typeIds, offsets, children };
      return isSparse ? new SparseUnionBatch(options) : new DenseUnionBatch(options);
    }

    // unsupported type
    default:
      throw new Error(`Unsupported type: ${typeId}, (${keyFor(Type, typeId)})`);
  }
}
