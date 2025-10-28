/**
 * @import { Message, MessageHeader_, Version_ } from '../types.js'
 */
import { MessageHeader, Version } from '../constants.js';
import { keyFor } from '../util/objects.js';
import { SIZEOF_INT, readInt16, readInt32, readInt64, readObject, readOffset, readUint8 } from '../util/read.js';
import { decodeDictionaryBatch } from './dictionary-batch.js';
import { decodeRecordBatch } from './record-batch.js';
import { decodeSchema } from './schema.js';

const invalidMessageMetadata = (expected, actual) =>
  `Expected to read ${expected} metadata bytes, but only read ${actual}.`;

const invalidMessageBodyLength = (expected, actual) =>
  `Expected to read ${expected} bytes for message body, but only read ${actual}.`;

const invalidMessageType = (type) =>
  `Unsupported message type: ${type} (${keyFor(MessageHeader, type)})`;

/**
 * A "message" contains a block of Apache Arrow data, such as a schema,
 * record batch, or dictionary batch. This message decodes a single
 * message, returning its associated metadata and content.
 * @param {Uint8Array} buf A byte buffer of binary Arrow IPC data
 * @param {number} index The starting index in the byte buffer
 * @returns {Message} The decoded message.
 */
export function decodeMessage(buf, index) {
  // get message start
  let metadataLength = readInt32(buf, index) || 0;
  index += SIZEOF_INT;

  // ARROW-6313: If the first 4 bytes are continuation indicator (-1), read
  // the next 4 for the 32-bit metadata length. Otherwise, assume this is a
  // pre-v0.15 message, where the first 4 bytes are the metadata length.
  if (metadataLength === -1) {
    metadataLength = readInt32(buf, index) || 0;
    index += SIZEOF_INT;
  }
  if (metadataLength === 0) return null;

  const head = buf.subarray(index, index += metadataLength);
  if (head.byteLength < metadataLength) {
    throw new Error(invalidMessageMetadata(metadataLength, head.byteLength));
  }

  // decode message metadata
  //  4: version
  //  6: headerType
  //  8: headerIndex
  // 10: bodyLength
  const get = readObject(head, 0);
  const version = /** @type {Version_} */
    (get(4, readInt16, Version.V1));
  const type = /** @type {MessageHeader_} */
    (get(6, readUint8, MessageHeader.NONE));
  const offset = get(8, readOffset, 0);
  const bodyLength = get(10, readInt64, 0);
  let content;

  if (offset) {
    // decode message header
    const decoder = type === MessageHeader.Schema ? decodeSchema
      : type === MessageHeader.DictionaryBatch ? decodeDictionaryBatch
      : type === MessageHeader.RecordBatch ? decodeRecordBatch
      : null;
    if (!decoder) throw new Error(invalidMessageType(type));
    content = decoder(head, offset, version);

    // extract message body
    if (bodyLength > 0) {
      const body = buf.subarray(index, index += bodyLength);
      if (body.byteLength < bodyLength) {
        throw new Error(invalidMessageBodyLength(bodyLength, body.byteLength));
      }
      // @ts-ignore
      content.body = body;
    } else if (type !== MessageHeader.Schema) {
      // table-from-ipc.js buffer accessor requires body to exist, even for empty batches
      // @ts-ignore
      content.body = new Uint8Array(0);
    }
  }

  return { version, type, index, content };
}
