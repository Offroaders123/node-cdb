'use strict';

var events = require('events'),
    fs     = require('fs'),
    util   = require('util'),
    _      = require('./cdb-util'),
    HEADER_SIZE = 2048,
    TABLE_SIZE = 256;

// Writable CDB definition
var writable = function(/** @type {string} */ file) {
    this.file = file;
    this.filePosition = 0;

    this.header = /** @type {{ position: number; slots: number; }[]} */ (new Array(TABLE_SIZE));
    this.hashtables = /** @type {{ hash: number; position: number; }[][]} */ (new Array(TABLE_SIZE));

    this.recordStream = /** @type {fs.WriteStream | null} */ (null);
    this.hashtableStream = /** @type {fs.WriteStream | null} */ (null);
};

module.exports = writable;

// extend EventEmitter for emit()
util.inherits(writable, events.EventEmitter);

writable.prototype.open = function(/** @type {(error: Error | null, writable?: typeof this & events.EventEmitter) => void} */ cb) {
    var recordStream = fs.createWriteStream(this.file, {start: HEADER_SIZE}),
        callback = cb || function() {},
        self = /** @type {typeof this & events.EventEmitter} */ (this);

    function fileOpened() {
        self.recordStream = recordStream;
        self.filePosition = HEADER_SIZE;

        recordStream.on('drain', function echoDrain() {
            self.emit('drain');
        });

        recordStream.removeListener('error', error);

        self.emit('open');
        callback(null, self);
    }

    /**
     * @param {Error} err
     */
    function error(err) {
        recordStream.removeListener('open', fileOpened);

        self.emit('error', err);
        callback(err);
    }

    recordStream.once('open', fileOpened);
    recordStream.once('error', error);
};

writable.prototype.put = function(/** @type {string} */ key, /** @type {string} */ data, /** @type {((error: Error | null | undefined) => void) | undefined} */ callback) {
    var keyLength = Buffer.byteLength(key),
        dataLength = Buffer.byteLength(data),
        record = new Buffer(8 + keyLength + dataLength),
        hash = _.cdbHash(key),
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

writable.prototype.close = function(/** @type {(error?: Error) => void} */ cb) {
    var self = /** @type {typeof this & events.EventEmitter} */ (this),
        callback = cb || function() {};

    this.recordStream.on('finish', openStreamForHashtable);
    this.recordStream.end();

    function openStreamForHashtable() {
        self.hashtableStream = fs.createWriteStream(self.file,
            {start: self.filePosition, flags: 'r+'});

        self.hashtableStream.once('open', writeHashtables);
        self.hashtableStream.once('error', error);
    }

    function writeHashtables() {
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

    function writeHeader() {
        var buffer = getBufferForHeader(self.header);

        fs.writeFile(self.file, buffer, {flag: 'r+'}, finished);
    }

    function finished() {
        self.emit('finish');
        callback();
    }

    /**
     * @param {Error} err
     */
    function error(err) {
        self.emit('error', err);
        callback(err);
    }
};

// === Helper functions ===

/**
 * Returns an allocated buffer containing the binary representation of a CDB
 * hashtable. Hashtables are linearly probed, and use a load factor of 0.5, so
 * the buffer will have 2n slots for n entries.
 * 
 * Entries are made up of two 32-bit unsigned integers for a total of 8 bytes.
 * 
 * @param {{ hash: number; position: number; }[]} hashtable
 */
function getBufferForHashtable(hashtable) {
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

/*
 * Returns an allocated buffer containing the binary representation of a CDB
 * header. The header contains 255 (count, position) pairs representing the
 * number of slots and position of the hashtables.
 */
/**
 * @param {{ position: number; slots: number; }[]} headerTable
 */
function getBufferForHeader(headerTable) {
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
