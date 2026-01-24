import * as lz4js from 'lz4js';
import { getCompressionCodec, setCompressionCodec } from '../../src/index.js';
import { CompressionType } from '../../src/constants.js';

export async function setCompressionCodecs() {
  if (getCompressionCodec(CompressionType.LZ4_FRAME) === null) {
    setCompressionCodec(CompressionType.LZ4_FRAME, {
      encode: (data) => lz4js.compress(data),
      decode: (data) => lz4js.decompress(data)
    });
  }
  if (getCompressionCodec(CompressionType.ZSTD) === null) {
    const { ZstdCodec } = await import('zstd-codec');
    await new Promise((resolve) => {
      ZstdCodec.run((zstd) => {
        const simple = new zstd.Simple();
        setCompressionCodec(CompressionType.ZSTD, {
          encode: (data) => simple.compress(data),
          decode: (data) => simple.decompress(data)
        });
        resolve();
      });
    });
  }
}

export function clearCompressionCodecs() {
  setCompressionCodec(CompressionType.LZ4_FRAME, undefined);
  setCompressionCodec(CompressionType.ZSTD, undefined);
}
