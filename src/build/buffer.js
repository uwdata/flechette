import { uint8Array } from '../util/arrays.js';

export function array(length, arrayType = uint8Array) {
  return new arrayType(
    length < 0 ? 1024 : align64(length, arrayType.BYTES_PER_ELEMENT)
  );
}

function align64(length, bpe = 1) {
  return (((length * bpe) + 7) & ~7) / bpe;
}

export function align(array, length = array.length) {
  const alignedLength = align64(length, array.BYTES_PER_ELEMENT);
  return array.length > alignedLength ? array.subarray(0, alignedLength)
    : array.length < alignedLength ? resize(array, alignedLength)
    : array;
}

export function resize(array, newLength) {
  const newArray = new array.constructor(newLength);
  newArray.set(array, array.length);
  return newArray;
}

export function grow(array, minLength) {
  // TODO: more efficient approach than looping
  // TODO: reuse logic in flatbuffer builder
  while (array.length < minLength) {
    array = resize(array, array.length << 1);
  }
  return array;
}

export function buffer(arrayType) {
  return new Buffer(arrayType);
}

export class Buffer {
  constructor(arrayType = uint8Array) {
    this.buf = new arrayType(512);
  }
  array(size) {
    return align(this.buf, size);
  }
  prep(index) {
    if (index >= this.buf.length) {
      this.buf = grow(this.buf, index);
    }
  }
  get(index) {
    return this.buf[index];
  }
  set(value, index) {
    this.prep(index);
    this.buf[index] = value;
  }
  write(bytes, index) {
    this.prep(index + bytes.length);
    this.buf.set(bytes, index);
  }
}

export function bitmap(size) {
  return new Bitmap(size);
}

export class Bitmap extends Buffer {
  set(index) {
    const i = index >> 3;
    this.prep(i);
    this.buf[i] |= (1 << (index % 8));
  }
}
