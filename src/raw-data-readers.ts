// Helpers for reading raw data
// Should consider separating this file to a new package
const fs = require('fs');
const doAsync = require('doasync');

const asyncFs = doAsync(fs);

// Readers should implement the "read" function, and optionally an async open function and an async close function.

class RawDataFileReader {
  /**
   * @param {string} filename
   */
  constructor(filename) {
    this.filename = filename;
    this.fd = /** @type {number} */ (null);
  }

  async open() {
    this.fd = await asyncFs.open(this.filename, 'r');
  }

  /**
   * @param {number} start
   * @param {number} length
   * @returns {Promise<Buffer>}
   */
  async read(start, length) {
    const self = this;
    const { buffer } = await asyncFs.read(self.fd, Buffer.alloc(length), 0, length, start);
    return buffer;
  }

  async close() {
    return /** @type {number} */ (asyncFs.close(this.fd));
  }
}

class RawDataBufferReader {
  /**
   * @param {Buffer} buffer
   */
  constructor(buffer) {
    this.buffer = buffer;
  }

  /**
   * @param {number} start
   * @param {number} length
   */
  async read(start, length) {
    return this.buffer.slice(start, start + length);
  }
}

/**
 * @typedef {{ read(start: number, length: number): Promise<Buffer>; open?(): Promise<void>; close?(): Promise<number>; }} CustomRawDataReader
*/

/**
 * @template {CustomRawDataReader} T
 * @param {string | Buffer | T} reader
 * @returns {T}
 */
function castToRawDataReader(reader) {
  if (typeof reader === 'string') {
    // @ts-expect-error
    return new RawDataFileReader(reader);
  }
  if (Buffer.isBuffer(reader)) {
    // @ts-expect-error
    return new RawDataBufferReader(reader);
  }
  if (!reader
  || (typeof reader.read !== 'function')
  || (reader.open && (typeof reader.open !== 'function'))
  || (reader.close && (typeof reader.close !== 'function'))) {
    throw new TypeError('Invalid raw-data reader - must have a read() function and if open and close are defined they should be functions');
  }
  return reader;
}

/**
 * @param {number} a
 * @param {number} b
 */
function quotient(a, b) { // floored division
  return (a - (a % b)) / b;
}

class RawDataReaderCacheWrapper {
  /**
   * @param {RawDataFileReader} reader
   */
  constructor(reader, { blockSize = 4096, blocksLimit = 2000 } = {}) {
    this.reader = castToRawDataReader(reader);
    this.blockSize = blockSize;
    this.blocksLimit = blocksLimit;
    this.newCache = new Map();
    this.oldCache = new Map();
  }

  async open() {
    if (this.reader.open) {
      return this.reader.open();
    }
    return null;
  }

  async close() {
    if (this.reader.close) {
      return this.reader.close();
    }
    return null;
  }

  /**
   * @param {number} index
   */
  async readBlock(index) {
    const cachedBlock = this.newCache.get(index);
    if (cachedBlock) {
      return cachedBlock;
    }
    const oldCachedBlock = this.oldCache.get(index);
    if (oldCachedBlock) {
      this.oldCache.delete(index);
    }
    const block = oldCachedBlock || await this.reader.read(index * this.blockSize, this.blockSize);
    if (this.newCache.size >= this.blocksLimit / 2) {
      this.oldCache = this.newCache;
      this.newCache = new Map();
    }
    this.newCache.set(index, block);
    return block;
  }

  /**
   * @param {number} start
   * @param {number} length
   */
  async read(start, length) {
    const startIndex = quotient(start, this.blockSize);
    const end = start + length;
    const endIndex = quotient(end + this.blockSize - 1, this.blockSize);
    const buffers = await Promise.all(Array.from({ length: endIndex - startIndex }, (_empty, index) => this.readBlock(startIndex + index)));
    return Buffer.concat(buffers).slice(start - startIndex * this.blockSize, end - startIndex * this.blockSize);
  }
}

exports.castToRawDataReader = castToRawDataReader;
exports.RawDataFileReader = RawDataFileReader;
exports.RawDataBufferReader = RawDataBufferReader;
exports.RawDataReaderCacheWrapper = RawDataReaderCacheWrapper;
