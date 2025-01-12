import { EOS, MAGIC, MessageHeader } from '../constants.js';
import { Builder } from './builder.js';
import { encodeDictionaryBatch } from './dictionary-batch.js';
import { writeFooter } from './footer.js';
import { encodeRecordBatch } from './record-batch.js';
import { encodeSchema } from './schema.js';
import { writeMessage } from './message.js';
import { MemorySink } from './sink.js';

const STREAM = 'stream';
const FILE = 'file';

/**
 * Encode assembled data into Arrow IPC binary format.
 * @param {any} data Assembled table data.
 * @param {object} options Encoding options.
 * @param {import('./sink.js').Sink} [options.sink] IPC byte consumer.
 * @param {'stream' | 'file'} [options.format] Arrow stream or file format.
 * @returns {import('./sink.js').Sink} The sink that was passed in.
 */
export function encodeIPC(data, { sink, format = STREAM } = {}) {
  if (format !== STREAM && format !== FILE) {
    throw new Error(`Unrecognized Arrow IPC format: ${format}`);
  }
  const { schema, dictionaries = [], records = [], metadata } = data;
  const builder = new Builder(sink || new MemorySink());
  const file = format === FILE;
  const dictBlocks = [];
  const recordBlocks = [];

  if (file) {
    builder.addBuffer(MAGIC);
  } else if (schema) {
    writeMessage(
      builder,
      MessageHeader.Schema,
      encodeSchema(builder, schema),
      0
    );
  }

  for (const dict of dictionaries) {
    const { data } = dict;
    writeMessage(
      builder,
      MessageHeader.DictionaryBatch,
      encodeDictionaryBatch(builder, dict),
      data.byteLength,
      dictBlocks
    );
    writeBuffers(builder, data.buffers);
  }

  for (const batch of records) {
    writeMessage(
      builder,
      MessageHeader.RecordBatch,
      encodeRecordBatch(builder, batch),
      batch.byteLength,
      recordBlocks
    );
    writeBuffers(builder, batch.buffers);
  }

  if (file) {
    writeFooter(builder, schema, dictBlocks, recordBlocks, metadata);
  } else {
    builder.addBuffer(EOS);
  }

  return builder.sink;
}

/**
 * Write byte buffers to the builder sink.
 * Buffers are aligned to 64 bits (8 bytes) as needed.
 * @param {import('./builder.js').Builder} builder
 * @param {Uint8Array[]} buffers
 */
function writeBuffers(builder, buffers) {
  for (let i = 0; i < buffers.length; ++i) {
    builder.addBuffer(buffers[i]); // handles alignment for us
  }
}
