import { describe, it, expect, beforeEach } from "vitest";
import { readFile } from 'node:fs/promises';
import { compress as lz4_compress, decompress as lz4_decompress } from 'lz4js';
import { ZstdCodec } from 'zstd-codec';
import { CompressionType, setCompressionCodec, tableFromIPC, tableToIPC } from "../src/index.js";

function setCodecLZ4() {
  setCompressionCodec(CompressionType.LZ4_FRAME, {
    encode: (data) => lz4_compress(data),
    decode: (data) => lz4_decompress(data)
  });
}

async function setCodecZSTD() {
  await new Promise((resolve) => {
    ZstdCodec.run((zstd) => {
      const codec = new zstd.Simple();
      setCompressionCodec(CompressionType.ZSTD, {
        encode: (data) => codec.compress(data),
        decode: (data) => codec.decompress(data)
      });
      resolve();
    });
  });
}

describe('compression codecs', () => {
  beforeEach(() => {
    // clear registered compression codecs
    setCompressionCodec(CompressionType.LZ4_FRAME, undefined);
    setCompressionCodec(CompressionType.ZSTD, undefined);
  });

  it('throws if codec is not registered', async () => {
    const lz4 = new Uint8Array(await readFile(`test/data/compressed_lz4.arrow`));
    const zstd = new Uint8Array(await readFile(`test/data/compressed_zstd.arrow`));
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    const table = tableFromIPC(none);
    expect(() => tableFromIPC(lz4)).throws();
    expect(() => tableFromIPC(zstd)).throws();
    expect(() => tableToIPC(table, { codec: CompressionType.LZ4_FRAME })).throws();
    expect(() => tableToIPC(table, { codec: CompressionType.ZSTD })).throws();
  });

  it('decode lz4 compressed buffers', async () => {
    setCodecLZ4();
    const lz4 = new Uint8Array(await readFile(`test/data/compressed_lz4.arrow`));
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    expect(tableFromIPC(lz4).toColumns()).toStrictEqual(tableFromIPC(none).toColumns());
  });

  it('decode zstd compressed buffers', async () => {
    await setCodecZSTD();
    const zstd = new Uint8Array(await readFile(`test/data/compressed_zstd.arrow`));
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    expect(tableFromIPC(zstd).toColumns()).toStrictEqual(tableFromIPC(none).toColumns());
  });

  it('encode lz4 compressed buffers', async () => {
    setCodecLZ4();
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    const table = tableFromIPC(none);
    const lz4 = tableToIPC(table, { codec: CompressionType.LZ4_FRAME });
    expect(tableFromIPC(lz4).toColumns()).toStrictEqual(table.toColumns());
  });

  it('encode zstd compressed buffers', async () => {
    await setCodecZSTD();
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    const table = tableFromIPC(none);
    const zstd = tableToIPC(table, { codec: CompressionType.ZSTD });
    expect(tableFromIPC(zstd).toColumns()).toStrictEqual(table.toColumns());
  });
});
