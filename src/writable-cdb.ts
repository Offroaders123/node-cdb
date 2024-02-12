import * as fs from 'fs';
import * as doAsync from 'doasync';
import {
  pointerEncoding,
  slotIndexEncoding,
  keyLengthEncoding,
  dataLengthEncoding,
  hashEncoding,
  TABLE_SIZE,
  HEADER_SIZE,
  MAIN_PAIR_SIZE,
  HASH_PAIR_SIZE,
  RECORD_HEADER_SIZE,
  defaultHash,
} from './cdb-util';

const asyncFs = doAsync(fs);

export interface Hashtable {
  hash: bigint;
  position: number;
}

// === Helper functions ===

/**
* Returns an allocated buffer containing the binary representation of a CDB
* hashtable. Hashtables are linearly probed, and use a load factor of 0.5, so
* the buffer will have 2n slots for n entries.
*
* Entries are made up of two 32-bit unsigned integers for a total of 8 bytes.
*/
function getBufferForHashtable(hashtable: Hashtable[]): Buffer {
  const { length } = hashtable;
  const slotCount = length * 2;
  const buffer = Buffer.alloc(slotCount * HASH_PAIR_SIZE);

  // zero out the buffer
  buffer.fill(0);

  for (let i = 0; i < length; i += 1) {
    const { hash, position } = hashtable[i];

    // eslint-disable-next-line no-bitwise
    let slot = Number((hash >> 8n) % BigInt(slotCount));
    // console.log(`*********** getBufferForHashtable checking empty slot ${slot}`);
    let bufferPosition = slot * HASH_PAIR_SIZE;

    // look for an empty slot
    while (pointerEncoding.read(buffer, bufferPosition + hashEncoding.size) !== 0) {
      // this slot is occupied
      slot = (slot + 1) % slotCount;
      bufferPosition = slot * HASH_PAIR_SIZE;
      // console.log(`*********** getBufferForHashtable slot was not empty, checking empty slot ${slot}`);
    }

    // console.log(`*********** getBufferForHashtable bufferPosition: ${bufferPosition} pointing to hash: 0x${hash.toString(16)} position: 0x${position.toString(16)}`);
    hashEncoding.write(buffer, hash, bufferPosition);
    pointerEncoding.write(buffer, position, bufferPosition + hashEncoding.size);
  }

  return buffer;
}

export interface WriteHeader {
  position: number;
  slots: number;
}

export interface RecordStreamDrainWaiter {
  resolve(value: void | PromiseLike<void>): void;
  reject(reason?: any): void;
}

class Writable {
  file: string;
  filePosition: number;
  hash: typeof defaultHash;
  header: WriteHeader[];
  hashtables: Hashtable[][];
  hashtableStream: fs.WriteStream | null;
  recordStream: fs.WriteStream | null;
  recordStreamError: Error | null;
  _recordStreamErrorSaver;
  _recordStreamDrainWaiters: Set<RecordStreamDrainWaiter>;
  _recordStreamDrainCaller;

  constructor(file: string, hash = defaultHash) {
    this.file = file;
    this.filePosition = 0;
    this.hash = hash;

    this.header = new Array(TABLE_SIZE);
    this.hashtables = new Array(TABLE_SIZE);

    this.hashtableStream = null;
    this.recordStream = null;
    this.recordStreamError = null;
    this._recordStreamErrorSaver = (err: Error): void => {
      this.recordStreamError = err;
      const waiters = this._recordStreamDrainWaiters;
      this._recordStreamDrainWaiters = new Set();
      waiters.forEach(({ reject }) => reject(err));
    };
    this._recordStreamDrainWaiters = new Set();
    this._recordStreamDrainCaller = (): void => {
      // listeners should be safe functions
      // for safety
      const waiters = this._recordStreamDrainWaiters;
      this._recordStreamDrainWaiters = new Set();
      waiters.forEach(({ resolve }) => resolve());
    };
  }

  async open(): Promise<Writable> {
    // console.log(`*********** opening file for writing: ${this.file} at start 0x${HEADER_SIZE.toString(16)}`);
    const recordStream = fs.createWriteStream(this.file, { start: HEADER_SIZE });

    return new Promise<Writable>((resolve, reject): void => {
      let alreadyFinished = false;

      const onceOpen = (): void => {
        if (alreadyFinished) {
          return;
        }
        this.recordStream = recordStream;
        this.filePosition = HEADER_SIZE;
        recordStream.once('error', this._recordStreamErrorSaver);
        recordStream.on('drain', this._recordStreamDrainCaller);
        recordStream.removeListener('error', onceError);
        alreadyFinished = true;
        resolve(this);
      };

      const onceError = (err: Error): void => {
        if (alreadyFinished) {
          return;
        }
        recordStream.removeListener('open', onceOpen);
        alreadyFinished = true;
        reject(err);
      };

      recordStream.once('error', onceError);
      recordStream.once('open', onceOpen);
    });
  }

  async put(keyParam: string | Buffer, dataParam: string | Buffer): Promise<void> {
    const key = Buffer.from(keyParam);
    const data = Buffer.from(dataParam);
    if (this.recordStreamError) {
      throw this.recordStreamError; // if set write operations are no longer permitted
    }
    const record = Buffer.alloc(RECORD_HEADER_SIZE + key.length + data.length);
    const hash = this.hash(key);
    // eslint-disable-next-line no-bitwise
    const hashtableIndex = hash & 0xFFn;

    keyLengthEncoding.write(record, key.length, 0);
    dataLengthEncoding.write(record, data.length, keyLengthEncoding.size);
    key.copy(record, RECORD_HEADER_SIZE);
    data.copy(record, RECORD_HEADER_SIZE + key.length);

    // console.log(`*********** writing key ${key} data ${data} record ${record.toString('hex')} to file position 0x${this.filePosition.toString(16)}`);
    const drainPromise = this.recordStream.write(record) ? null : new Promise<void>((resolve, reject): void => {
      this._recordStreamDrainWaiters.add({ resolve, reject });
      // We don't wait for the entire flush
    });


    let hashtable = this.hashtables[Number(hashtableIndex)];
    if (!hashtable) {
      hashtable = [];
      this.hashtables[Number(hashtableIndex)] = hashtable;
    }

    hashtable.push({ hash, position: this.filePosition });

    this.filePosition += record.length;

    if (drainPromise) {
      await drainPromise;
      if (this.recordStreamError) {
        throw this.recordStreamError; // check if was an error during the writing
      }
    }
  }

  async close(): Promise<void> {
    await new Promise<void>((resolve, reject): void => {
      let alreadyFinished = false;
      const onFinish = (): void => {
        if (alreadyFinished) {
          return;
        }
        alreadyFinished = true;
        this.recordStream.removeListener('error', onError);
        resolve();
      };
      const onError = (err: Error): void => {
        if (alreadyFinished) {
          return;
        }
        alreadyFinished = true;
        this.recordStream.removeListener('finish', onFinish);
        reject(err);
      };
      if (this.recordStreamError) {
        reject(this.recordStreamError); // check if was an error during the writing
        return;
      }
      // Let's replace the error handler
      this.recordStream.once('finish', onFinish);
      this.recordStream.once('error', onError);
      this.recordStream.removeListener('error', this._recordStreamErrorSaver);
      this.recordStream.end();
    });
    this.recordStream.removeListener('drain', this._recordStreamDrainCaller);

    await new Promise<void>((resolve, reject): void => {
      let alreadyFinished = false;
      const onOpen = (): void => {
        if (alreadyFinished) {
          return;
        }

        const { length } = this.hashtables;
        for (let i = 0; i < length; i += 1) {
          const hashtable = this.hashtables[i] || [];
          const buffer = getBufferForHashtable(hashtable);

          if (buffer.length > 0) {
            // console.log(`*********** writing the buffer at 0x${this.filePosition.toString(16)}`);
            this.hashtableStream.write(buffer);
          }

          this.header[i] = {
            position: this.filePosition,
            slots: hashtable.length * 2,
          };

          this.filePosition += buffer.length;

          // free the hashtable
          this.hashtables[i] = null;
        }
        this.hashtableStream.once('finish', onFinish);
        this.hashtableStream.end();
      };
      const onFinish = (): void => {
        if (alreadyFinished) {
          return;
        }
        alreadyFinished = true;
        this.hashtableStream.removeListener('error', onError);
        resolve();
      };
      const onError = (err: Error): void => {
        if (alreadyFinished) {
          return;
        }
        alreadyFinished = true;
        this.hashtableStream.removeListener('open', onOpen);
        this.hashtableStream.removeListener('finish', onFinish);
        reject(err);
      };
      // hashtableStream could be local but we save it on this for debugging
      // console.log(`*********** opening file for hashtable writing: ${this.file} at start 0x${self.filePosition.toString(16)}`);
      this.hashtableStream = fs.createWriteStream(this.file, { start: this.filePosition, flags: 'r+' });
      this.hashtableStream.once('open', onOpen);
      this.hashtableStream.once('error', onError);
    });

    /*
    * Allocated buffer containing the binary representation of a CDB
    * header. The header contains 255 (count, position) pairs representing the
    * number of slots and position of the hashtables.
    */
    const buffer = Buffer.alloc(HEADER_SIZE);
    let bufferPosition = 0;

    for (let i = 0; i < TABLE_SIZE; i += 1) {
      const { position, slots } = this.header[i];

      pointerEncoding.write(buffer, position, bufferPosition);
      slotIndexEncoding.write(buffer, slots, bufferPosition + pointerEncoding.size); // 4 bytes per int
      bufferPosition += MAIN_PAIR_SIZE;
    }

    await asyncFs.writeFile(this.file, buffer, { flag: 'r+' });
  }
}

export { Writable };
