const uInt32LE = {
  size: 4,
  read: (buffer: Buffer, offset = 0): number => buffer.readUInt32LE(offset),
  write: (buffer: Buffer, value: number, offset = 0): void => {
    buffer.writeUInt32LE(value, offset);
  },
} as const;

const uInt64LE = {
  size: 8,
  // eslint-disable-next-line no-bitwise
  read: (buffer: Buffer, offset = 0): number => Number(BigInt(buffer.readUInt32LE(offset)) + (BigInt(buffer.readUInt32LE(offset + 4)) << 32n)),
  write: (buffer: Buffer, value: number, offset = 0): void => {
    const bigValue = BigInt(value);
    // eslint-disable-next-line no-bitwise
    buffer.writeUInt32LE(Number(bigValue & 0xFFFFFFFFn), offset);
    // eslint-disable-next-line no-bitwise
    buffer.writeUInt32LE(Number(bigValue >> 32n), offset + 4);
  },
} as const;

const uInt64LEBigInt = {
  size: 8,
  // eslint-disable-next-line no-bitwise
  read: (buffer: Buffer, offset = 0): bigint => BigInt(buffer.readUInt32LE(offset)) + (BigInt(buffer.readUInt32LE(offset + 4)) << 32n),
  write: (buffer: Buffer, value: bigint, offset = 0): void => {
    // eslint-disable-next-line no-bitwise
    buffer.writeUInt32LE(Number(value & 0xFFFFFFFFn), offset);
    // eslint-disable-next-line no-bitwise
    buffer.writeUInt32LE(Number(value >> 32n), offset + 4);
  },
} as const;

export const pointerEncoding = uInt64LE;
export const slotIndexEncoding = uInt64LE;

export const keyLengthEncoding = uInt32LE;
export const dataLengthEncoding = uInt32LE;

export const hashEncoding = uInt64LEBigInt;

export const TABLE_SIZE = 256;
export const HEADER_SIZE: number = TABLE_SIZE * (pointerEncoding.size + slotIndexEncoding.size);
export const MAIN_PAIR_SIZE: number = pointerEncoding.size + slotIndexEncoding.size;
export const HASH_PAIR_SIZE: number = hashEncoding.size + pointerEncoding.size;
export const RECORD_HEADER_SIZE: number = keyLengthEncoding.size + dataLengthEncoding.size;

// hash functions must return a BigInt
export function originalHash(key: Buffer): bigint {
  // DJB hash
  const { length } = key;
  let hash = 5381;

  for (let i = 0; i < length; i += 1) {
    // eslint-disable-next-line no-bitwise
    hash = ((((hash << 5) >>> 0) + hash) ^ key[i]!) >>> 0;
  }

  // console.log(`*********** hash is: 0x${hash.toString(16)}`);
  return BigInt(hash);
}

export function defaultHash(key: Buffer): bigint {
  // Using all of our 8 byte hash in the simplest way possible.
  let paddedKey = key;
  if (key.length < 4) {
    paddedKey = Buffer.alloc(4);
    key.copy(paddedKey);
  }
  // eslint-disable-next-line no-bitwise
  return originalHash(key) + (BigInt(paddedKey.readUInt32LE(0)) << 32n);
}
