// Helpers for reading raw data
// Should consider separating this file to a new package
import * as fs from 'node:fs';
import * as doAsync from 'doasync';

const asyncFs = doAsync(fs);

// Readers should implement the "read" function, and optionally an async open function and an async close function.

export class RawDataFileReader {
  filename: string;
  fd: number | null;

  constructor(filename: string) {
    this.filename = filename;
    this.fd = null;
  }

  async open(): Promise<void> {
    this.fd = await asyncFs.open(this.filename, 'r');
  }

  async read(start: number, length: number): Promise<Buffer> {
    const self = this;
    const { buffer } = await asyncFs.read(self.fd, Buffer.alloc(length), 0, length, start);
    return buffer;
  }

  async close(): Promise<number> {
    return asyncFs.close(this.fd) as number;
  }
}

export class RawDataBufferReader {
  buffer: Buffer;

  constructor(buffer: Buffer) {
    this.buffer = buffer;
  }

  async read(start: number, length: number): Promise<Buffer> {
    return this.buffer.slice(start, start + length);
  }
}

export interface CustomRawDataReader {
  read(start: number, length: number): Promise<Buffer>;
  open?(): Promise<void>;
  close?(): Promise<number>;
}

export function castToRawDataReader<T extends CustomRawDataReader>(reader: string | Buffer | T): T {
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

function quotient(a: number, b: number): number { // floored division
  return (a - (a % b)) / b;
}

export class RawDataReaderCacheWrapper {
  reader: CustomRawDataReader;
  blockSize: number;
  blocksLimit: number;
  newCache: Map<number, Buffer>;
  oldCache: Map<number, Buffer>;

  constructor(reader: string | Buffer | CustomRawDataReader, { blockSize = 4096, blocksLimit = 2000 } = {}) {
    this.reader = castToRawDataReader(reader);
    this.blockSize = blockSize;
    this.blocksLimit = blocksLimit;
    this.newCache = new Map();
    this.oldCache = new Map();
  }

  async open(): Promise<void | null> {
    if (this.reader.open) {
      return this.reader.open();
    }
    return null;
  }

  async close(): Promise<number | null> {
    if (this.reader.close) {
      return this.reader.close();
    }
    return null;
  }

  async readBlock(index: number): Promise<Buffer> {
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

  async read(start: number, length: number): Promise<Buffer> {
    const startIndex = quotient(start, this.blockSize);
    const end = start + length;
    const endIndex = quotient(end + this.blockSize - 1, this.blockSize);
    const buffers = await Promise.all(Array.from({ length: endIndex - startIndex }, (_empty, index) => this.readBlock(startIndex + index)));
    return Buffer.concat(buffers).slice(start - startIndex * this.blockSize, end - startIndex * this.blockSize);
  }
}
