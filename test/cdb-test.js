// @ts-check

const vows = require('vows');
const assert = require('assert');
const fs = require('fs');
const toCallback = require('./to-callback');
const { Writable } = require('../dist/writable-cdb');
const { Readable } = require('../dist/readable-cdb');

const tempFile = 'test/tmp.tmp';
const fakeFile = 'test/doesntexist.tmp';

try {
  fs.unlinkSync(tempFile);
} catch (err) { // eslint-disable-line no-empty
}

vows.describe('cdb-test').addBatch({
  'A writable cdb': {
    topic() {
      return new Writable(tempFile);
    },

    'should not create a file when instantiated': () => {
      assert.throws(() => {
        fs.statSync(tempFile);
      }, Error);
    },

    'should respond to put': (cdb) => {
      assert.isFunction(cdb.put);
    },

    'if written but not opened': {
      topic(cdb) {
        toCallback(cdb.put('a', 'b'), this.callback);
      },

      'should error': (err, putResult) => { // eslint-disable-line no-unused-vars
        assert.notEqual(err, null);
      },
    },

    'when opened': {
      topic(cdb) {
        toCallback(cdb.open(), this.callback);
      },

      'should not error': (err, cdb) => { // eslint-disable-line no-unused-vars
        assert.equal(err, null);
      },

      'should create a file': (err, cdb) => { // eslint-disable-line no-unused-vars
        assert.isObject(fs.statSync(tempFile));
      },

      'should add records without exception': (cdb) => { // eslint-disable-line no-unused-vars
        assert.doesNotThrow(() => {
          cdb.put('meow', '0xdeadbeef');
          cdb.put('meow', '0xbeefdead');
          cdb.put('abcd', 'test1');
          cdb.put('abcd', 'offset_test');
          cdb.put('efgh', 'test2');
          cdb.put('ijkl', 'test3');
          cdb.put('mnopqrs', 'test4');
        }, Error);
      },

      'should close': {
        topic(cdb) {
          toCallback(cdb.close(), this.callback);
        },

        'without error': (err) => {
          assert.equal(err, null);
        },

        'and have a file with non-zero size': (err) => { // eslint-disable-line no-unused-vars
          const stat = fs.statSync(tempFile);
          assert.isObject(stat);
          assert.isTrue(stat.size !== 0);
        },
      },

    },
  },
}).addBatch({
  'A readable cdb': {
    'for a non-existing file': {
      topic() {
        return new Readable(fakeFile);
      },

      'when opened': {
        topic(cdb) {
          toCallback(cdb.open(), this.callback);
        },

        'should error': (err, cdb) => { // eslint-disable-line no-unused-vars
          assert.notEqual(err, null);
        },
      },
    },

    'for an existing file': {
      topic() {
        return new Readable(tempFile);
      },

      'when opened': {
        topic(cdb) {
          toCallback(cdb.open(), this.callback);
        },

        'should not error': (err, cdb) => { // eslint-disable-line no-unused-vars
          assert.equal(err, null);
        },

        'should return an iterator': {
          topic(cdb) {
            return cdb.getIterator('meow');
          },

          'and return the right first element': {
            topic(iterator) {
              toCallback(iterator.next(), this.callback);
            },

            'without error': (err, data) => { // eslint-disable-line no-unused-vars
              assert.equal(err, null);
            },

            'with the right data': (err, data) => {
              assert.equal(data.value, '0xdeadbeef');
            },

            'with a duplicate': {
              topic(data, iterator) {
                toCallback(iterator.next(), this.callback);
              },

              'that is found via iterator.next()': (err, data) => { // eslint-disable-line no-unused-vars
                assert.equal(err, null);
                assert.equal(data.value, '0xbeefdead');
              },

              'and the next iteration': {
                topic(secondData, firstData, iterator) {
                  toCallback(iterator.next(), this.callback);
                },

                'is empty': (err, data) => {
                  assert.equal(err, null);
                  assert.equal(data.value, null);
                  assert.isTrue(data.done);
                },
              },
            },
          },
        },

        'should find an existing key at an offset': {
          topic(cdb) {
            toCallback(cdb.get('abcd', 1), this.callback);
          },

          'without error': (err, data) => { // eslint-disable-line no-unused-vars
            assert.equal(err, null);
          },

          'and return the right data': (err, data) => { // eslint-disable-line no-unused-vars
            assert.equal(data, 'offset_test');
          },
        },

        'should not find a missing key': {
          topic(cdb) {
            toCallback(cdb.get('kitty cat'), this.callback);
          },

          'and should not error': (err, data) => { // eslint-disable-line no-unused-vars
            assert.equal(err, null);
          },

          'and should have a null result': (err, data) => { // eslint-disable-line no-unused-vars
            assert.equal(data, null);
          },
        },
      },

      teardown(cdb) {
        toCallback(cdb.close(), this.callback);
      },
    },

    'for an open existing file': {
      topic() {
        toCallback((new Readable(tempFile)).open(), this.callback);
      },

      'when closed': {
        topic(cdb) {
          toCallback(cdb.close(), this.callback);
        },

        'should not error': (err, _) => { // eslint-disable-line no-unused-vars
          assert.equal(err, null);
        },
      },
    },

    teardown() {
      fs.unlinkSync(tempFile);
    },
  },
}).addBatch({
  'The CDB package\'s module.exports': {
    topic() {
      // eslint-disable-next-line global-require
      return require('../');
    },

    'should have a writable CDB': (index) => {
      assert.isFunction(index.Writable);
    },

    'should have a readable CDB': (index) => {
      assert.isFunction(index.Readable);
    },
  },
}).export(module);
