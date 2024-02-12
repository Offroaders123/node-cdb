import * as fs from 'fs';
import * as _  from './cdb-util';
var HEADER_SIZE = 2048,
    TABLE_SIZE  = 256;

export class readable {
constructor(/** @type {string} */ file) {
    this.file = file;
    this.header = new Array(TABLE_SIZE);

    this.fd = null;
    this.bookmark = /** @type {((callback: (error: Error | null, buffer?: Buffer | null) => void) => void) | null} */ (null);
};

open(/** @type {(error: NodeJS.ErrnoException, readable?: typeof this) => void} */ callback) {
    var self = this;

    fs.open(this.file, 'r', readHeader);

    /**
     * @param {NodeJS.ErrnoException | null} err
     * @param {number} fd
     */
    function readHeader(err, fd) {
        if (err) {
            return callback(err);
        }

        self.fd = fd;
        fs.read(fd, new Buffer(HEADER_SIZE), 0, HEADER_SIZE, 0, parseHeader);
    }

    /**
     * @param {NodeJS.ErrnoException | null} err
     * @param {number} _bytesRead
     * @param {Buffer} buffer
     */
    function parseHeader(err, _bytesRead, buffer) {
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

get(/** @type {string} */ key, /** @type {((error: Error | null, buffer?: Buffer | null) => void) | number} */ offset, /** @type {((err: Error | null, buffer?: Buffer | null) => void) | undefined} */ callback) {
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

    /**
     * @param {number} slot
     */
    function readSlot(slot) {
        hashPosition = position + ((slot % slotCount) * 8);

        fs.read(self.fd, new Buffer(8), 0, 8, hashPosition, checkHash);
    }

    /**
     * @param {Error} err
     * @param {number} _bytesRead
     * @param {Buffer} buffer
    */
    function checkHash(err, _bytesRead, buffer) {
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

    /**
     * @param {Error} err
     * @param {number} _bytesRead
     * @param {Buffer} buffer
     */
    function readKey(err, _bytesRead, buffer) {
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

    /**
     * @param {Error} err
     * @param {number} _bytesRead
     * @param {Buffer} buffer
     */
    function checkKey(err, _bytesRead, buffer) {
        if (err) {
            return callback(err);
        }

        if (buffer.toString() == key && offset === 0) {
            fs.read(self.fd, new Buffer(dataLength), 0, dataLength,
                recordPosition + 8 + keyLength, returnData);
        } else if (offset !== 0) {
            /** @type {number} */ (offset)--;
            readSlot(++slot);
        } else {
            readSlot(++slot);
        }
    }

    /**
     * @param {Error} err
     * @param {number} _bytesRead
     * @param {Buffer} buffer
     */
    function returnData(err, _bytesRead, buffer) {
        // Fill out bookmark information so getNext() will work
        self.bookmark = function(newCallback) {
            callback = newCallback;
            readSlot(++slot);
        };

        callback(err, buffer);
    }
};

getNext(/** @type {(error: Error | null, buffer?: Buffer | null) => void} */ callback) {
    if (this.bookmark) {
        this.bookmark(callback);
    }
};

close(/** @type {fs.NoParamCallback} */ callback) {
    fs.close(this.fd, callback);
};
}
