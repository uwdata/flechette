import { performance } from 'node:perf_hooks';

export function timeit(fn) {
  const t0 = performance.now();
  const value = fn();
  const t1 = performance.now();
  return { time: t1 - t0, value };
}

export function benchmark(fn, iter = 10) {
  const times = [];
  for (let i = 0; i < iter; ++i) {
    times.push(timeit(fn).time);
  }
  return stats(times);
}

function stats(times) {
  const iter = times.length;
  const init = times[0];
  let sum = init;
  let min = init;
  let max = init;
  for (let i = 1; i < iter; ++i) {
    const t = times[i];
    sum += t;
    if (t < min) min = t;
    if (t > max) max = t;
  }
  const avg = sum / iter;
  return { avg, init, min, max };
}
