/**
 * @import { ArrowData, BodyCompression, ExtractionOptions, Field, RecordBatch, Schema } from '../types.js'
 */
import { batchType } from '../batch-type.js';
import { Column, columnBuilder } from '../column.js';
import { decompressBuffer, getCompressionCodec, missingCodec } from '../compression.js';
import { BodyCompressionMethod, Type, UnionMode, Version } from '../constants.js';
import { invalidDataType } from '../data-types.js';
import { Table } from '../table.js';
import { int8Array } from '../util/arrays.js';
import { decodeIPC } from './decode-ipc.js';

/**
 * Decode [Apache Arrow IPC data][1] and return a new Table. The input binary
 * data may be either an `ArrayBuffer` or `Uint8Array`. For Arrow data in the
 * [IPC 'stream' format][2], an array of `Uint8Array` values is also supported.
 *
 * [1]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
 * [2]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 * @param {ArrayBufferLike | Uint8Array | Uint8Array[]} data
 *  The source byte buffer, or an array of buffers. If an array, each byte
 *  array may contain one or more self-contained messages. Messages may NOT
 *  span multiple byte arrays.
 * @param {ExtractionOptions} [options]
 *  Options for controlling how values are transformed when extracted
 *  from an Arrow binary representation.
 * @returns {Table} A Table instance.
 */
export function tableFromIPC(data, options) {
  return createTable(decodeIPC(data), options);
}

/**
 * Create a table from parsed IPC data.
 * @param {ArrowData} data
 *  The IPC data, as returned by parseIPC.
 * @param {ExtractionOptions} [options]
 *  Options for controlling how values are transformed when extracted
 *  from am Arrow binary representation.
 * @returns {Table} A Table instance.
 */
export function createTable(data, options = {}) {
  const { schema = { fields: [] }, dictionaries, records, dictsBeforeRecord } = data;
  const { version, fields } = schema;
  // `dictionaryMap` is closed over by `context()` and read by `visit()` when
  // it encounters a Dictionary-typed field on a record batch. Each record
  // batch captures whatever Column reference is current in `dictionaryMap`
  // at the time it is decoded — exactly mirroring how arrow-js's
  // `_loadDictionaryBatch` calls `this.dictionaries.set(header.id, vector)`
  // and how `_loadRecordBatch` then resolves dict references against the
  // current `this.dictionaries` Map.
  const dictionaryMap = new Map;
  const context = contextGenerator(options, version, dictionaryMap);

  // build dictionary type map
  const dictionaryTypes = new Map;
  visitSchemaFields(schema, field => {
    const type = field.type;
    if (type.typeId === Type.Dictionary) {
      dictionaryTypes.set(type.id, type.dictionary);
    }
  });

  /**
   * Process a single dictionary batch by either replacing or extending the
   * Column currently registered for its id in `dictionaryMap`. A fresh
   * Column instance is always created (never mutated in place) so that any
   * record batch that previously captured the old Column reference is
   * unaffected.
   *
   * Mirrors arrow-js's `_loadDictionaryBatch`:
   *   return (dictionary && isDelta
   *     ? dictionary.concat(new Vector(data))
   *     : new Vector(data)).memoize();
   * @param {import('../types.js').DictionaryBatch} dict
   */
  const processDict = (dict) => {
    const { id, data: dictData, isDelta, body } = dict;
    const type = dictionaryTypes.get(id);
    const batch = visit(type, context({ ...dictData, body }));
    const existing = dictionaryMap.get(id);
    if (!existing) {
      if (isDelta) {
        throw new Error('Delta update can not be first dictionary batch.');
      }
      dictionaryMap.set(id, new Column([batch], type));
    } else if (isDelta) {
      // delta — append to the existing dictionary column
      dictionaryMap.set(id, new Column([...existing.data, batch], type));
    } else {
      // non-delta replacement — start fresh
      dictionaryMap.set(id, new Column([batch], type));
    }
  };

  // Decode dictionary and record batches in their original stream order so
  // that each record batch sees the dictionaries that were current at its
  // position. `dictsBeforeRecord[i]` is the number of dictionary batches
  // that preceded record batch `i` in the stream/file; if it is missing
  // (legacy callers building `ArrowData` by hand), fall back to processing
  // all dictionary batches before any record batch.
  const cols = fields.map(f => columnBuilder(f.type));
  let dictIdx = 0;
  for (let i = 0; i < records.length; i++) {
    const target = dictsBeforeRecord ? dictsBeforeRecord[i] : dictionaries.length;
    while (dictIdx < target) {
      processDict(dictionaries[dictIdx++]);
    }

    const ctx = context(records[i]);
    fields.forEach((f, idx) => cols[idx].add(visit(f.type, ctx)));
  }
  // Drain any dictionary batches that come after the last record batch in
  // the stream. These can't affect any already-decoded record batch, but
  // processing them surfaces any malformed-dict errors that the caller
  // would expect to see.
  while (dictIdx < dictionaries.length) {
    processDict(dictionaries[dictIdx++]);
  }

  return new Table(schema, cols.map(c => c.done()), options.useProxy);
}

/**
 * Visit all fields within a schema.
 * @param {Schema} schema
 * @param {(field: Field) => void} visitor
 */
function visitSchemaFields(schema, visitor) {
  schema.fields.forEach(function visitField(field) {
    visitor(field);
    // @ts-ignore
    field.type.dictionary?.children?.forEach(visitField);
    // @ts-ignore
    field.type.children?.forEach(visitField);
  });
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

  /**
   * Return a context generator.
   * @param {RecordBatch} batch
   */
  return batch => {
    const { length, nodes, regions, compression, variadic, body } = batch;
    let nodeIndex = -1;
    let bufferIndex = -1;
    let variadicIndex = -1;
    return {
      ...base,
      length,
      node: () => nodes[++nodeIndex],
      buffer: (ArrayType) => {
        const { bytes, length, offset } = maybeDecompress(body, regions[++bufferIndex], compression);
        return ArrayType
          ? new ArrayType(bytes.buffer, bytes.byteOffset + offset, length / ArrayType.BYTES_PER_ELEMENT)
          : bytes.subarray(offset, offset + length)
      },
      variadic: () => variadic[++variadicIndex],
      visit(children) { return children.map(f => visit(f.type, this)); }
    };
  };
}

/**
 * Prepare an arrow buffer for use, potentially decompressing it.
 * @param {Uint8Array} body
 * @param {{offset: number, length: number}} region
 * @param {BodyCompression} compression
 */
function maybeDecompress(body, region, compression) {
  if (!compression) {
    return { bytes: body, ...region };
  } else if (compression.method !== BodyCompressionMethod.BUFFER) {
    throw new Error(`Unknown compression method (${compression.method})`);
  } else {
    const id = compression.codec;
    const codec = getCompressionCodec(id);
    if (!codec) throw new Error(missingCodec(id));
    return decompressBuffer(body, region, codec);
  }
}

/**
 * Visit a field, instantiating views of buffer regions.
 */
function visit(type, ctx) {
  const { typeId } = type;
  const { options, node, buffer, variadic, version } = ctx;
  const BatchType = batchType(type, options);

  // extract the next { length, nullCount } field node - ALL fields have field nodes
  const base = { ...node(), type };

  if (typeId === Type.Null) {
    // null fields have field nodes but no data buffers
    return new BatchType({ ...base, nullCount: base.length });
  }

  switch (typeId) {
    // validity and data value buffers
    case Type.Bool:
    case Type.Int:
    case Type.Time:
    case Type.Duration:
    case Type.Float:
    case Type.Decimal:
    case Type.Date:
    case Type.Timestamp:
    case Type.Interval:
    case Type.FixedSizeBinary:
      return new BatchType({
        ...base,
        validity: buffer(),
        values: buffer(type.values)
      });

    // validity, offset, and value buffers
    case Type.Utf8:
    case Type.LargeUtf8:
    case Type.Binary:
    case Type.LargeBinary:
      return new BatchType({
        ...base,
        validity: buffer(),
        offsets: buffer(type.offsets),
        values: buffer()
      });

    // views with variadic buffers
    case Type.BinaryView:
    case Type.Utf8View:
      return new BatchType({
        ...base,
        validity: buffer(),
        values: buffer(), // views buffer
        data: Array.from({ length: variadic() }, () => buffer()) // data buffers
      });

    // validity, offset, and list child
    case Type.List:
    case Type.LargeList:
    case Type.Map:
      return new BatchType({
        ...base,
        validity: buffer(),
        offsets: buffer(type.offsets),
        children: ctx.visit(type.children)
      });

    // validity, offset, size, and list child
    case Type.ListView:
    case Type.LargeListView:
      return new BatchType({
        ...base,
        validity: buffer(),
        offsets: buffer(type.offsets),
        sizes: buffer(type.offsets),
        children: ctx.visit(type.children)
      });

    // validity and children
    case Type.FixedSizeList:
    case Type.Struct:
      return new BatchType({
        ...base,
        validity: buffer(),
        children: ctx.visit(type.children)
      });

    // children only
    case Type.RunEndEncoded:
      return new BatchType({
        ...base,
        children: ctx.visit(type.children)
      });

    // dictionary
    case Type.Dictionary: {
      const { id, indices } = type;
      return new BatchType({
        ...base,
        validity: buffer(),
        values: buffer(indices.values),
      }).setDictionary(ctx.dictionary(id));
    }

    // union
    case Type.Union: {
      if (version < Version.V5) {
        buffer(); // skip unused null bitmap
      }
      return new BatchType({
        ...base,
        typeIds: buffer(int8Array),
        offsets: type.mode === UnionMode.Sparse ? null : buffer(type.offsets),
        children: ctx.visit(type.children)
      });
    }

    // unsupported type
    default:
      throw new Error(invalidDataType(typeId));
  }
}
