/**
 * @import { ArrowData, ExtractionOptions, Field, RecordBatch, Schema } from '../types.js'
 */
import { batchType } from '../batch-type.js';
import { columnBuilder } from '../column.js';
import { Type, UnionMode, Version } from '../constants.js';
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
  const { schema = { fields: [] }, dictionaries, records } = data;
  const { version, fields } = schema;
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

  // decode dictionaries, build dictionary column map
  const dicts = new Map;
  for (const dict of dictionaries) {
    const { id, data, isDelta, body } = dict;
    const type = dictionaryTypes.get(id);
    const batch = visit(type, context({ ...data, body }));
    if (!dicts.has(id)) {
      if (isDelta) {
        throw new Error('Delta update can not be first dictionary batch.');
      }
      dicts.set(id, columnBuilder(type).add(batch));
    } else {
      const dict = dicts.get(id);
      if (!isDelta) dict.clear();
      dict.add(batch);
    }
  }
  dicts.forEach((value, key) => dictionaryMap.set(key, value.done()));

  // decode column fields
  const cols = fields.map(f => columnBuilder(f.type));
  for (const batch of records) {
    const ctx = context(batch);
    fields.forEach((f, i) => cols[i].add(visit(f.type, ctx)));
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
    const { length, nodes, regions, variadic, body } = batch;
    let nodeIndex = -1;
    let bufferIndex = -1;
    let variadicIndex = -1;
    return {
      ...base,
      length,
      node: () => nodes[++nodeIndex],
      buffer: (ArrayType) => {
        const { length, offset } = regions[++bufferIndex];
        return ArrayType
          ? new ArrayType(body.buffer, body.byteOffset + offset, length / ArrayType.BYTES_PER_ELEMENT)
          : body.subarray(offset, offset + length)
      },
      variadic: () => variadic[++variadicIndex],
      visit(children) { return children.map(f => visit(f.type, this)); }
    };
  };
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
