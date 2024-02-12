import * as fs from 'node:fs';
import * as _  from './cdb-util.js';
var HEADER_SIZE = 2048,
    TABLE_SIZE  = 256;

export class readable {

file: string;
header: { position: number; slotCount: number; }[];
fd: number | null;
bookmark: ((callback: (error: Error | null, buffer?: Buffer | null) => void) => void) | null;

constructor(file: string) {
    this.file = file;
    this.header = new Array(TABLE_SIZE);

    this.fd = null;
    this.bookmark = null;
};

open(callback: (error: NodeJS.ErrnoException, readable?: typeof this) => void): void {
    var self = this;

    fs.open(this.file, 'r', readHeader);

    function readHeader(err: NodeJS.ErrnoException | null, fd: number): void {
        if (err) {
            return callback(err);
        }

        self.fd = fd;
        fs.read(fd, new Buffer(HEADER_SIZE), 0, HEADER_SIZE, 0, parseHeader);
    }

    function parseHeader(err: NodeJS.ErrnoException | null, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback(err);
        }

        var bufferPosition = 0,
            i, position, slotCount;

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

get(key: string, offset: ((error: Error | null, buffer?: Buffer | null) => void) | number, callback: ((err: Error | null, buffer?: Buffer | null) => void) | undefined): void {
    var hash = _.cdbHash(key),
        hashtableIndex = hash & 255,
        hashtable = this.header[hashtableIndex],
        position = hashtable.position,
        slotCount = hashtable.slotCount,
        slot = (hash >>> 8) % slotCount,
        trueKeyLength = Buffer.byteLength(key),
        self = this,
        hashPosition, recordHash, recordPosition = 0, keyLength = 0, dataLength = 0;

    if (typeof(offset) == 'function') {
        callback = offset;
        offset = 0;
    }

    if (slotCount === 0) {
        return callback(null, null);
    }

    readSlot(slot);

    function readSlot(slot: number): void {
        hashPosition = position + ((slot % slotCount) * 8);

        fs.read(self.fd, new Buffer(8), 0, 8, hashPosition, checkHash);
    }

    function checkHash(err: Error, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback(err);
        }

        recordHash = buffer.readUInt32LE(0);
        recordPosition = buffer.readUInt32LE(4);

        if (recordHash == hash) {
            fs.read(self.fd, new Buffer(8), 0, 8, recordPosition, readKey);
        } else if (recordHash === 0) {
            callback(null, null);
        } else {
            readSlot(++slot);
        }
    }

    function readKey(err: Error, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback(err);
        }

        keyLength = buffer.readUInt32LE(0);
        dataLength = buffer.readUInt32LE(4);

        // In the rare case that there is a hash collision, check the key size
        // to prevent reading in a key that will definitely not match.
        if (keyLength != trueKeyLength) {
            return readSlot(++slot);
        }

        fs.read(self.fd, new Buffer(keyLength), 0, keyLength,
            recordPosition + 8, checkKey);
    }

    function checkKey(err: Error, _bytesRead: number, buffer: Buffer): void {
        if (err) {
            return callback(err);
        }

        if (buffer.toString() == key && offset === 0) {
            fs.read(self.fd, new Buffer(dataLength), 0, dataLength,
                recordPosition + 8 + keyLength, returnData);
        } else if (offset !== 0) {
            (offset as number)--;
            readSlot(++slot);
        } else {
            readSlot(++slot);
        }
    }

    function returnData(err: Error, _bytesRead: number, buffer: Buffer): void {
        // Fill out bookmark information so getNext() will work
        self.bookmark = function(newCallback) {
            callback = newCallback;
            readSlot(++slot);
        };

        callback(err, buffer);
    }
};

getNext(callback: (error: Error | null, buffer?: Buffer | null) => void): void {
    if (this.bookmark) {
        this.bookmark(callback);
    }
};

close(callback: fs.NoParamCallback): void {
    fs.close(this.fd, callback);
};
}
