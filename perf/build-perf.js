import { Bool, DateDay, Dictionary, Float64, Int32, Utf8, vectorFromArray } from 'apache-arrow';
import { bool, columnFromArray, dateDay, dictionary, float64, int32, tableToIPC, utf8 } from '../src/index.js';
import { table } from '../src/table.js';
import { bools, dates, floats, ints, sample, strings, uniqueStrings } from './data.js';
import { benchmark } from './util.js';

function fl(data, typeKey) {
  const type = typeKey === 'bool' ? bool()
    : typeKey === 'int' ? int32()
    : typeKey === 'float' ? float64()
    : typeKey === 'utf8' ? utf8()
    : typeKey === 'date' ? dateDay()
    : typeKey === 'dict' ? dictionary(utf8(), int32())
    : null;
  return columnFromArray(data, type);
}

function aa(data, typeKey) {
  const type = typeKey === 'bool' ? new Bool()
    : typeKey === 'int' ? new Int32()
    : typeKey === 'float' ? new Float64()
    : typeKey === 'utf8' ? new Utf8()
    : typeKey === 'utf8' ? new DateDay()
    : typeKey === 'dict' ? new Dictionary(new Utf8(), new Int32())
    : null;
  return vectorFromArray(data, type);
}

function js(data) {
  return JSON.stringify(data);
}

function run(N, nulls, msg, iter = 5) {
  const int = ints(N, -10000, 10000, nulls);
  const float = floats(N, -10000, 10000, nulls);
  const bool = bools(N, nulls);
  const utf8 = strings(N, nulls);
  const date = dates(N, nulls);
  const dict = sample(N, uniqueStrings(100), nulls);

  console.log(`\n** Build performance for ${msg} **\n`);
  trial('Build int column', int, 'int', iter);
  trial('Build float column', float, 'float', iter);
  trial('Build bool column', bool, 'bool', iter);
  trial('Build date column', date, 'date', iter);
  trial('Build utf8 column', utf8, 'utf8', iter);
  trial('Build dict utf8 column', dict, 'dict', iter);
}

function trial(task, data, typeKey, iter) {
  const jl = new TextEncoder().encode(JSON.stringify(data)).length;
  const al = tableToIPC(table({ v: fl(data, typeKey) })).length;
  const sz = `json ${(jl/1e6).toFixed(1)} MB, arrow ${(al/1e6).toFixed(1)} MB`;

  console.log(`${task} (${typeKey}, ${iter} iteration${iter === 1 ? '' : 's'}, ${sz})`);
  const j = benchmark(() => js(data, typeKey), iter);
  const a = benchmark(() => aa(data, typeKey), iter);
  const f = benchmark(() => fl(data, typeKey), iter);
  const p = Object.keys(a);

  console.table(p.map(key => ({
    measure: key,
    json: +(j[key].toFixed(2)),
    'arrow-js': +(a[key].toFixed(2)),
    flechette: +(f[key].toFixed(2)),
    ratio: +((a[key] / f[key]).toFixed(2))
  })));
}

run(1e6, 0, '1M values');
run(1e6, 0.05, '1M values, 5% nulls');
