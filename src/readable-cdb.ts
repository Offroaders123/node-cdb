import { CustomRawDataReader, castToRawDataReader } from './raw-data-readers';
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

export interface ReadHeader {
  position: number;
  slotCount: number;
}

class Readable {
  reader: CustomRawDataReader;
  header: ReadHeader[];
  hash: typeof defaultHash;

  constructor(reader: string | Buffer | CustomRawDataReader, hash = defaultHash) {
    this.reader = castToRawDataReader(reader);
    this.header = new Array(TABLE_SIZE);
    this.hash = hash;
  }

  async readRaw(start: number, length: number): Promise<Buffer> {
    const buffer = await this.reader.read(start, length);
    if (buffer.length < length) {
      throw new Error('Unexpected end of buffer or file');
    }
    return buffer;
  }

  async open(): Promise<this> {
    if (this.reader.open) {
      await this.reader.open();
    }
    const buffer = await this.readRaw(0, HEADER_SIZE);

    let bufferPosition = 0;
    for (let i = 0; i < TABLE_SIZE; i += 1) {
      const position = pointerEncoding.read(buffer, bufferPosition);
      const slotCount = slotIndexEncoding.read(buffer, bufferPosition + pointerEncoding.size);

      this.header[i] = {
        position,
        slotCount,
      };

      bufferPosition += MAIN_PAIR_SIZE;
    }

    return this;
  }

  async* getIterator(keyParam: string | Buffer, offsetParam = 0): AsyncGenerator<Buffer, undefined, void> {
    // console.log(`*********** Readable.get ${key} offset: ${offsetParam}`);
    const key = Buffer.from(keyParam);

    const hash = this.hash(key);
    // eslint-disable-next-line no-bitwise
    const { position, slotCount } = this.header[Number(hash & 0xFFn)];
    // console.log(`*********** position ${position} slotCount: ${slotCount}`);

    let offset = offsetParam;

    if (slotCount === 0) {
      return;
    }

    // eslint-disable-next-line no-bitwise
    const initialSlot = Number((hash >> 8n) % BigInt(slotCount));

    for (let slotIndex = 0; slotIndex < slotCount; slotIndex += 1) {
      const hashPosition = position + (((initialSlot + slotIndex) % slotCount) * HASH_PAIR_SIZE);
      // console.log(`*********** reading slot ${slotIndex} at ${hashPosition}`);

      // eslint-disable-next-line no-await-in-loop
      const slotBuffer = await this.readRaw(hashPosition, HASH_PAIR_SIZE);

      const recordHash = hashEncoding.read(slotBuffer, 0);
      const recordPosition = pointerEncoding.read(slotBuffer, hashEncoding.size);
      // console.log(`*********** recordHash 0x${recordHash.toString(16)} recordPosition 0x${recordPosition.toString(16)}`);

      if (recordPosition === 0) {
        // console.log(`*********** did not find data because an empty record was reached ${recordPosition} ${recordHash}`);
        return;
      }
      // console.log(`*********** found hash 0x${hash.toString(16)}`);
      if (recordHash === hash) {
        // eslint-disable-next-line no-await-in-loop
        const recordHeader = await this.readRaw(recordPosition, RECORD_HEADER_SIZE);

        const keyLength = keyLengthEncoding.read(recordHeader, 0);
        const dataLength = dataLengthEncoding.read(recordHeader, keyLengthEncoding.size);

        // In the rare case that there is a hash collision, check the key size
        // to prevent reading in a key that will definitely not match.
        // console.log(`*********** keyLength ${keyLength} trueKeyLength ${trueKeyLength}`);
        if (keyLength === key.length) {
          // eslint-disable-next-line no-await-in-loop
          const keyPayload = await this.readRaw(recordPosition + RECORD_HEADER_SIZE, keyLength);
          // console.log(`*********** found key ${keyPayload}`);
          if (Buffer.compare(keyPayload, key) === 0) {
            // console.log(`*********** same key - offset is ${offset}`);
            if (offset === 0) {
              // eslint-disable-next-line no-await-in-loop
              yield await this.readRaw(recordPosition + RECORD_HEADER_SIZE + keyLength, dataLength);
            } else {
              // console.log(`*********** reducing offset ${offset} by 1`);
              offset -= 1;
            }
          }
        }
      }
    }
    // console.log(`*********** did not find data because all records have been scanned ${slotCount}`);
  }

  async get(key: string, offset = 0): Promise<Buffer | undefined> {
    return (await this.getIterator(key, offset).next()).value;
  }

  async close(): Promise<number> {
    if (this.reader.close) {
      return this.reader.close();
    }
    return null;
  }
}

export { Readable };
