import { EventEmitter } from 'node:events';
import { createWriteStream, writeFile } from 'node:fs';
import { cdbHash } from './cdb-util.js';

import type { WriteStream } from 'node:fs';

var HEADER_SIZE = 2048,
    TABLE_SIZE = 256;

export interface WriteHeader {
    position: number;
    slots: number;
}

export interface Hashtable {
    hash: number;
    position: number;
}

// Writable CDB definition
export class CDBWritable extends EventEmitter {

file: string;
filePosition: number;
header: WriteHeader[];
hashtables: Hashtable[][];
recordStream: WriteStream | null;
hashtableStream: WriteStream | null;

constructor(file: string) {
    super();
    this.file = file;
    this.filePosition = 0;

    this.header = new Array(TABLE_SIZE);
    this.hashtables = new Array(TABLE_SIZE);

    this.recordStream = null;
    this.hashtableStream = null;
};

open(cb: (error: Error | null, writable?: typeof this) => void): void {
    var recordStream = createWriteStream(this.file, {start: HEADER_SIZE}),
        callback = cb || function() {},
        self = this;

    function fileOpened(): void {
        self.recordStream = recordStream;
        self.filePosition = HEADER_SIZE;

        recordStream.on('drain', function echoDrain() {
            self.emit('drain');
        });

        recordStream.removeListener('error', error);

        self.emit('open');
        callback(null, self);
    }

    function error(err: Error): void {
        recordStream.removeListener('open', fileOpened);

        self.emit('error', err);
        callback(err);
    }

    recordStream.once('open', fileOpened);
    recordStream.once('error', error);
};

put(key: string, data: string, callback: (error: Error) => void): boolean {
    var keyLength = Buffer.byteLength(key),
        dataLength = Buffer.byteLength(data),
        record = new Buffer(8 + keyLength + dataLength),
        hash = cdbHash(key),
        hashtableIndex = hash & 255,
        hashtable = this.hashtables[hashtableIndex],
        okayToWrite;

    record.writeUInt32LE(keyLength, 0);
    record.writeUInt32LE(dataLength, 4);
    record.write(key, 8);
    record.write(data, 8 + keyLength);

    okayToWrite = this.recordStream.write(record, callback);

    if (!hashtable) {
        this.hashtables[hashtableIndex] = hashtable = [];
    }

    hashtable.push({hash: hash, position: this.filePosition});

    this.filePosition += record.length;

    return okayToWrite;
};

close(cb: (error?: Error) => void): void {
    var self = this,
        callback = cb || function() {};

    this.recordStream.on('finish', openStreamForHashtable);
    this.recordStream.end();

    function openStreamForHashtable(): void {
        self.hashtableStream = createWriteStream(self.file,
            {start: self.filePosition, flags: 'r+'});

        self.hashtableStream.once('open', writeHashtables);
        self.hashtableStream.once('error', error);
    }

    function writeHashtables(): void {
        var length = self.hashtables.length,
            i = 0, hashtable, buffer;

        for (i = 0; i < length; i++) {
            hashtable = self.hashtables[i] || [];
            buffer = getBufferForHashtable(hashtable);

            self.hashtableStream.write(buffer);

            self.header[i] = {
                position: self.filePosition,
                slots: hashtable.length * 2
            };

            self.filePosition += buffer.length;

            // free the hashtable
            self.hashtables[i] = null;
        }

        self.hashtableStream.on('finish', writeHeader);
        self.hashtableStream.end();
    }

    function writeHeader(): void {
        var buffer = getBufferForHeader(self.header);

        writeFile(self.file, buffer, {flag: 'r+'}, finished);
    }

    function finished(): void {
        self.emit('finish');
        callback();
    }

    function error(err: Error): void {
        self.emit('error', err);
        callback(err);
    }
};
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
    var length = hashtable.length,
        slotCount = length * 2,
        buffer = new Buffer(slotCount * 8),
        i, hash, position, slot, bufferPosition;

    // zero out the buffer
    buffer.fill(0);

    for (i = 0; i < length; i++) {
        hash = hashtable[i].hash;
        position = hashtable[i].position;

        slot = (hash >>> 8) % slotCount;
        bufferPosition = slot * 8;

        // look for an empty slot
        while (buffer.readUInt32LE(bufferPosition) !== 0) {
            // this slot is occupied
            slot = (slot + 1) % slotCount;
            bufferPosition = slot * 8;
        }

        buffer.writeUInt32LE(hash, bufferPosition);
        buffer.writeUInt32LE(position, bufferPosition + 4);
    }

    return buffer;
}

/**
 * Returns an allocated buffer containing the binary representation of a CDB
 * header. The header contains 255 (count, position) pairs representing the
 * number of slots and position of the hashtables.
 */
function getBufferForHeader(headerTable: WriteHeader[]): Buffer {
    var buffer = new Buffer(HEADER_SIZE),
        bufferPosition = 0,
        i, position, slots;

    for (i = 0; i < TABLE_SIZE; i++) {
        position = headerTable[i].position;
        slots = headerTable[i].slots;

        buffer.writeUInt32LE(position, bufferPosition);
        buffer.writeUInt32LE(slots, bufferPosition + 4); // 4 bytes per int
        bufferPosition += 8;
    }

    return buffer;
}
