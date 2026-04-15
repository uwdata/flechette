// Repro for a dictionary-replacement decoding bug in `tableFromIPC`.
//
// In the Arrow IPC stream format, a single dictionary id can be associated
// with multiple non-delta dictionary batches: each subsequent non-delta
// batch REPLACES the previous one for the purposes of any record batches
// that come AFTER it in the stream. Record batches always reference the
// dictionary that is current at their position in the stream.
//
// `decode/table-from-ipc.js`'s `createTable` decodes all dictionary batches
// in one pass before any record batches, overwriting the dictionary stored
// for an id when it sees a non-delta replacement. After the loop, every
// record batch — including ones that were transmitted BEFORE a replacement
// — gets associated with the FINAL dictionary for the id, losing the
// dictionary that was current at their position.
//
// The fixture `test/data/dict_replacement.arrows` is an Arrow IPC stream
// (written by Apache Arrow's Rust `StreamWriter`, which Apache Arrow JS
// reads correctly) with the following logical contents:
//
//   schema { id: Int64, name: Dict<UInt32, Utf8> }
//   dict batch (id=0, isDelta=false): values=["alice","bob","carol"]
//   record batch 1: id=[1,2,3], name keys=[0,1,2]
//   dict batch (id=0, isDelta=false): values=["dave","eve"]
//   record batch 2: id=[4,5],   name keys=[0,1]
//
// The expected decoded table is:
//
//   id | name
//    1 | alice
//    2 | bob
//    3 | carol
//    4 | dave
//    5 | eve
//
// Today's flechette behavior: rows 0..2 are resolved against the LAST
// dictionary `["dave","eve"]` instead of the dictionary that was current at
// the position of record batch 1, so the decoded table is wrong:
//
//   id | name
//    1 | dave        (key 0 → wrong dict)
//    2 | eve         (key 1 → wrong dict)
//    3 | undefined   (key 2 → out of bounds in 2-entry dict)
//    4 | dave
//    5 | eve

import { describe, it, expect } from 'vitest';
import { readFile } from 'node:fs/promises';
import { tableFromIPC } from '../src/index.js';

describe('tableFromIPC', () => {
  it('honors per-record-batch dictionary replacements', async () => {
    const bytes = new Uint8Array(await readFile('test/data/dict_replacement.arrows'));
    const table = tableFromIPC(bytes);

    const ids = table.getChild('id');
    const names = table.getChild('name');

    const expected = [
      { id: 1, name: 'alice' },
      { id: 2, name: 'bob' },
      { id: 3, name: 'carol' },
      { id: 4, name: 'dave' },
      { id: 5, name: 'eve' },
    ];

    const actual = [];
    for (let i = 0; i < table.numRows; i++) {
      actual.push({ id: ids.get(i), name: names.get(i) });
    }

    expect(actual).toStrictEqual(expected);
  });
});
