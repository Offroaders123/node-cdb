// @ts-check

const vows = require('vows');
const assert = require('assert');
const fs = require('fs');
const toCallback = require('./to-callback');
const { Writable } = require('../dist/writable-cdb');
const { Readable } = require('../dist/readable-cdb');
const { RawDataReaderCacheWrapper } = require('../dist/raw-data-readers');

const randomFile = 'test/random.tmp';

try {
  fs.unlinkSync(randomFile);
} catch (err) { // eslint-disable-line no-empty
}

const pseudoRandom = (() => {
  let seed = 1073741823;
  return () => {
    seed = (seed * 16807) % 2147483647;
    return (seed - 1) / 2147483646;
  };
})();

function getRandomInt(min, max) {
  return Math.floor(pseudoRandom() * (max - min)) + min;
}

function getRandomString(minLength, maxLength) {
  const length = getRandomInt(minLength, maxLength);
  const stringArray = [];
  for (let i = 0; i < length; i += 1) {
    stringArray.push(String.fromCharCode(getRandomInt(97, 122)));
  }

  return stringArray.join('');
}

function generateRandomRecords(count) {
  const randomRecords = {};
  for (let i = 0; i < count; i += 1) {
    const key = getRandomString(5, 10);
    const data = getRandomString(20, 30);

    if (key in randomRecords) {
      randomRecords[key].push(data);
    } else {
      randomRecords[key] = [data];
    }
  }

  return randomRecords;
}

function iterateOverRecords(records, callback) {
  const keys = Object.keys(records);
  for (let i = 0; i < keys.length; i += 1) {
    const key = keys[i];
    const data = records[key];
    for (let j = 0; j < data.length; j += 1) {
      callback(key, j, data[j]);
    }
  }
}

const recordCount = 1000;
const randomRecords = generateRandomRecords(recordCount);

function readBatch(readableCreator) {
  return {
    topic() {
      toCallback(readableCreator().open(), this.callback);
    },

    'should not error': (err, cdb) => { // eslint-disable-line no-unused-vars
      assert.equal(err, null);
    },

    'when searching for existing keys': {
      topic(cdb) {
        let found = 0;
        let notFound = 0;
        let count = recordCount;
        const { callback } = this;

        function checkRecord(expected) {
          return (err, data) => {
            if (err || !data || data.toString() !== expected) {
              notFound += 1;
            } else {
              found += 1;
            }
            count -= 1;
            if (count === 0) {
              callback(notFound, found);
            }
          };
        }

        iterateOverRecords(randomRecords, (key, offset, data) => {
          toCallback(cdb.get(key, offset), checkRecord(data));
        });
      },

      'should find all of them': (notFound, found) => {
        assert.equal(found, recordCount);
        assert.equal(notFound, null);
      },
    },

    teardown(cdb) {
      toCallback(cdb.close(), this.callback);
    },
  };
}

vows.describe('cdb-random-test')
.addBatch({
  'An opened writable cdb': {
    topic() {
      toCallback((new Writable(randomFile)).open(), this.callback);
    },

    'should not error': (err, cdb) => { // eslint-disable-line no-unused-vars
      assert.equal(err, null);
    },

    'should add records without exception': (err, cdb) => {
      assert.doesNotThrow(() => {
        iterateOverRecords(randomRecords, (key, offset, data) => {
          cdb.put(key, data);
        });
      }, Error);
    },

    'should close': {
      topic(cdb) {
        toCallback(cdb.close(), this.callback);
      },

      'without error': (err) => {
        assert.equal(err, null);
      },
    },
  },
})
.addBatch({
  'An opened readable cdb uncached': readBatch(() => new Readable(randomFile)),
  // Using small cache to test cache-cleanup
  'An opened readable cdb cached': readBatch(() => new Readable(new RawDataReaderCacheWrapper(randomFile, { blockSize: 100, blocksLimit: 10 }))),
  'An opened readable cdb buffer': readBatch(() => new Readable(fs.readFileSync(randomFile))),
  teardown() {
    fs.unlinkSync(randomFile);
  },
})
.export(module);
