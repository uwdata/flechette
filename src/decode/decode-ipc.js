/**
 * @import { ArrowData, Version_ } from '../types.js'
 */
import { MAGIC, MessageHeader, Version } from '../constants.js';
import { isArrayBufferLike } from '../util/arrays.js';
import { readInt16, readInt32, readObject } from '../util/read.js';
import { decodeBlocks } from './block.js';
import { decodeMessage } from './message.js';
import { decodeMetadata } from './metadata.js';
import { decodeSchema } from './schema.js';

/**
 * Decode [Apache Arrow IPC data][1] and return parsed schema, record batch,
 * and dictionary batch definitions. The input binary data may be either
 * an `ArrayBuffer` or `Uint8Array`. For Arrow data in the IPC 'stream' format,
 * an array of `Uint8Array` instances is also supported.
 *
 * This method stops short of generating views over field buffers. Use the
 * `createData()` method on the result to enable column data access.
 *
 * [1]: https://arrow.apache.org/docs/format/Columnar.html#serialization-and-interprocess-communication-ipc
 * @param {ArrayBufferLike | Uint8Array | Uint8Array[]} data
 *  The source byte buffer, or an array of buffers. If an array, each byte
 *  array may contain one or more self-contained messages. Messages may NOT
 *  span multiple byte arrays.
 * @returns {import('../types.js').ArrowData}
 */
export function decodeIPC(data) {
  const source = isArrayBufferLike(data) ? new Uint8Array(data) : data;
  return source instanceof Uint8Array && isArrowFileFormat(source)
    ? decodeIPCFile(source)
    : decodeIPCStream(source);
}

/**
 * @param {Uint8Array} buf
 * @returns {boolean}
 */
function isArrowFileFormat(buf) {
  if (!buf || buf.length < 4) return false;
  for (let i = 0; i < 6; ++i) {
    if (MAGIC[i] !== buf[i]) return false;
  }
  return true;
}

/**
 * Decode data in the [Arrow IPC 'stream' format][1].
 *
 * [1]: https://arrow.apache.org/docs/format/Columnar.html#ipc-streaming-format
 * @param {Uint8Array | Uint8Array[]} data The source byte buffer, or an
 *  array of buffers. If an array, each byte array may contain one or more
 *  self-contained messages. Messages may NOT span multiple byte arrays.
 * @returns {ArrowData}
 */
export function decodeIPCStream(data) {
  const stream = [data].flat();

  let schema;
  const records = [];
  const dictionaries = [];

  // consume each message in the stream
  for (const buf of stream) {
    if (!(buf instanceof Uint8Array)) {
      throw new Error(`IPC data batch was not a Uint8Array.`);
    }
    let offset = 0;

    // decode all messages in current buffer
    while (true) {
      const m = decodeMessage(buf, offset);
      if (m === null) break; // end of messages
      offset = m.index;
      if (!m.content) continue;
      switch (m.type) {
        case MessageHeader.Schema:
          // ignore repeated schema messages
          if (!schema) schema = m.content;
          break;
        case MessageHeader.RecordBatch:
          records.push(m.content);
          break;
        case MessageHeader.DictionaryBatch:
          dictionaries.push(m.content);
          break;
      }
    }
  }

  return /** @type {ArrowData} */ (
    { schema, dictionaries, records, metadata: null }
  );
}

/**
 * Decode data in the [Arrow IPC 'file' format][1].
 *
 * [1]: https://arrow.apache.org/docs/format/Columnar.html#ipc-file-format
 * @param {Uint8Array} data The source byte buffer.
 * @returns {ArrowData}
 */
export function decodeIPCFile(data) {
  // find footer location
  const offset = data.byteLength - (MAGIC.length + 4);
  const length = readInt32(data, offset);

  // decode file footer
  //  4: version
  //  6: schema
  //  8: dictionaries (vector)
  // 10: batches (vector)
  // 12: metadata
  const get = readObject(data, offset - length);
  const version = /** @type {Version_} */
    (get(4, readInt16, Version.V1));
  const dicts = get(8, decodeBlocks, []);
  const recs = get(10, decodeBlocks, []);

  return /** @type {ArrowData} */ ({
    schema: get(6, (buf, index) => decodeSchema(buf, index, version)),
    dictionaries: dicts.map(({ offset }) => decodeMessage(data, offset).content),
    records: recs.map(({ offset }) => decodeMessage(data, offset).content),
    metadata: get(12, decodeMetadata)
  });
}
