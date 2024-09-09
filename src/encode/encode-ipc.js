import { MAGIC, MessageHeader } from '../constants.js';
import { Builder } from './builder.js';
import { encodeDictionaryBatch } from './dictionary-batch.js';
import { writeFooter } from './footer.js';
import { encodeRecordBatch } from './record-batch.js';
import { encodeSchema } from './schema.js';
import { writeMessage } from './message.js';
import { MemorySink } from './sink.js';

/**
 * Encode assembled data into Arrow IPC binary format.
 * @param {any} data Assembled table data.
 * @param {object} options Encoding options.
 * @param {import('./sink.js').Sink} [options.sink] IPC byte consumer.
 * @param {'stream' | 'file'} [options.format] Arrow stream or file format.
 * @returns {import('./sink.js').Sink} The sink that was passed in.
 */
export function encodeIPC(data, { sink, format } = {}) {
  const { schema, dictionaries = [], records = [], metadata } = data;
  const builder = new Builder(sink || new MemorySink());
  const file = format === 'file';
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
  }

  return builder.sink;
}

function writeBuffers(builder, buffers) {
  // addBuffer handles 64-bit (8-byte) alignment
  for (let i = 0; i < buffers.length; ++i) {
    builder.addBuffer(buffers[i]);
  }
}

