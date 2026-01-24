import { describe, it, expect } from "vitest";
import { readFile } from 'node:fs/promises';
import { tableFromIPC } from "../src/index.js";
import { clearCompressionCodecs, setCompressionCodecs } from "./util/codecs.js";

describe('compression codecs', () => {
  it('throws if codec is not registered', async () => {
    const lz4 = new Uint8Array(await readFile(`test/data/compressed_lz4.arrow`));
    const zstd = new Uint8Array(await readFile(`test/data/compressed_zstd.arrow`));
    clearCompressionCodecs();
    expect(() => tableFromIPC(lz4)).throws();
    expect(() => tableFromIPC(zstd)).throws();
  });

  it('decode lz4 compressed buffers', async () => {
    await setCompressionCodecs();
    const lz4 = new Uint8Array(await readFile(`test/data/compressed_lz4.arrow`));
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    expect(tableFromIPC(lz4).toColumns()).toStrictEqual(tableFromIPC(none).toColumns());
  });

  it('decode zstd compressed buffers', async () => {
    await setCompressionCodecs();
    const zstd = new Uint8Array(await readFile(`test/data/compressed_zstd.arrow`));
    const none = new Uint8Array(await readFile(`test/data/compressed_none.arrow`));
    expect(tableFromIPC(zstd).toColumns()).toStrictEqual(tableFromIPC(none).toColumns());
  });
});
