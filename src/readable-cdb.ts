import { open, read, close } from 'node:fs';
import { cdbHash } from './cdb-util.js';

const HEADER_SIZE = 2048;
const TABLE_SIZE = 256;

export interface ReadHeader {
    position: number;
    slotCount: number;
}

export interface GetCallback {
    (error: Error | null, buffer?: null): void;
    (error: Error | null, buffer: Buffer): void;
}

export class CDBReadable {

file: string;
header: ReadHeader[] = Array(TABLE_SIZE);
fd!: number;
bookmark?: (callback: GetCallback) => void;

constructor(file: string) {
    this.file = file;
};

open(callback: (error: Error | null, readable?: typeof this) => void): void {
    const self = this;

    open(this.file, 'r', readHeader);

    function readHeader(err: Error | null, fd: number): void {
        if (err) {
            return callback(err);
        }

        self.fd = fd;
        read(fd, Buffer.alloc(HEADER_SIZE), 0, HEADER_SIZE, 0, parseHeader);
    }

    function parseHeader(err: Error | null, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback(err);
        }

        let bufferPosition = 0,
            i: number, position: number, slotCount: number;

        for (i = 0; i < TABLE_SIZE; i++) {
            position = buffer.readUInt32LE(bufferPosition);
            slotCount = buffer.readUInt32LE(bufferPosition + 4);

            self.header[i] = {
                position: position,
                slotCount: slotCount
            };

            bufferPosition += 8;
        }

        callback(null, self);
    }
};

get(key: string, offset: GetCallback): void;
get(key: string, offset: number, callback: GetCallback): void;
get(key: string, offset: GetCallback | number, callback?: GetCallback): void {
    const hash = cdbHash(key),
        hashtableIndex = hash & 255,
        hashtable = this.header[hashtableIndex]!,
        position = hashtable.position,
        slotCount = hashtable.slotCount;
    let slot = (hash >>> 8) % slotCount;
    const trueKeyLength = Buffer.byteLength(key),
        self = this;
    let hashPosition: number, recordHash: number, recordPosition: number, keyLength: number, dataLength: number;

    if (typeof(offset) == 'function') {
        callback = offset;
        offset = 0;
    }

    if (slotCount === 0) {
        return callback?.(null, null);
    }

    readSlot(slot);

    function readSlot(slot: number): void {
        hashPosition = position + ((slot % slotCount) * 8);

        read(self.fd, Buffer.alloc(8), 0, 8, hashPosition, checkHash);
    }

    function checkHash(err: Error | null, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback?.(err);
        }

        recordHash = buffer.readUInt32LE(0);
        recordPosition = buffer.readUInt32LE(4);

        if (recordHash == hash) {
            read(self.fd, Buffer.alloc(8), 0, 8, recordPosition, readKey);
        } else if (recordHash === 0) {
            callback?.(null, null);
        } else {
            readSlot(++slot);
        }
    }

    function readKey(err: Error | null, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback?.(err);
        }

        keyLength = buffer.readUInt32LE(0);
        dataLength = buffer.readUInt32LE(4);

        // In the rare case that there is a hash collision, check the key size
        // to prevent reading in a key that will definitely not match.
        if (keyLength != trueKeyLength) {
            return readSlot(++slot);
        }

        read(self.fd, Buffer.alloc(keyLength), 0, keyLength,
            recordPosition + 8, checkKey);
    }

    function checkKey(err: Error | null, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback?.(err);
        }

        if (buffer.toString() == key && offset === 0) {
            read(self.fd, Buffer.alloc(dataLength), 0, dataLength,
                recordPosition + 8 + keyLength, returnData);
        } else if (offset !== 0) {
            (offset as number)--;
            readSlot(++slot);
        } else {
            readSlot(++slot);
        }
    }

    function returnData(err: Error | null, _bytesRead: number, buffer: Buffer): void {
        // Fill out bookmark information so getNext() will work
        self.bookmark = function(newCallback) {
            callback = newCallback;
            readSlot(++slot);
        };

        callback?.(err, buffer);
    }
};

getNext(callback: GetCallback): void {
    if (this.bookmark) {
        this.bookmark(callback);
    }
};

close(callback: (error: Error | null) => void): void {
    close(this.fd, callback);
};
}
