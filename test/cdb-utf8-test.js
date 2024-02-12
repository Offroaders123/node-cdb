// @ts-check

const vows = require('vows');
const assert = require('assert');
const fs = require('fs');
const toCallback = require('./to-callback');
const Writable = require('../dist/writable-cdb');
const Readable = require('../dist/readable-cdb');

const tempFile = 'test/utf8.tmp';

try {
  fs.unlinkSync(tempFile);
} catch (err) { // eslint-disable-line no-empty
}

vows.describe('cdb-utf8-test').addBatch({
  'A writable cdb': {
    topic() {
      return new Writable(tempFile);
    },

    'when opened': {
      topic(cdb) {
        toCallback(cdb.open(), this.callback);
      },

      'should write UTF8 characters': {
        topic(cdb) {
          cdb.put('é', 'unicode test');
          cdb.put('€', 'unicode test');
          cdb.put('key', 'ᚠᛇᚻ');
          cdb.put('대한민국', '안성기');

          toCallback(cdb.close(), this.callback);
        },

        'and close successfully': (err) => {
          assert.equal(err, null);
        },
      },
    },
  },
}).addBatch({
  'A readable cdb should find that': {
    topic() {
      toCallback((new Readable(tempFile)).open(), this.callback);
    },

    é: {
      topic(cdb) {
        toCallback(cdb.get('é'), this.callback);
      },

      exists(err, data) {
        assert.isNull(err);
        assert.isNotNull(data);
      },

      'has the right value': (err, data) => {
        assert.equal(data, 'unicode test');
      },
    },

    '€': {
      topic(cdb) {
        toCallback(cdb.get('€'), this.callback);
      },

      exists(err, data) {
        assert.isNull(err);
        assert.isNotNull(data);
      },

      'has the right value': (err, data) => {
        assert.equal(data, 'unicode test');
      },
    },

    key: {
      topic(cdb) {
        toCallback(cdb.get('key'), this.callback);
      },

      exists(err, data) {
        assert.isNull(err);
        assert.isNotNull(data);
      },

      'has the right value': (err, data) => {
        assert.equal(data, 'ᚠᛇᚻ');
      },
    },

    대한민국: {
      topic(cdb) {
        toCallback(cdb.get('대한민국'), this.callback);
      },

      exists(err, data) {
        assert.isNull(err);
        assert.isNotNull(data);
      },

      'has the right value': (err, data) => {
        assert.equal(data, '안성기');
      },
    },

    teardown() {
      fs.unlinkSync(tempFile);
    },
  },

}).export(module);
