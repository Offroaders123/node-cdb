// @ts-check

module.exports = function toCallback(promise, callback) {
  Promise.resolve(promise)
  .then(value => [null, value])
  .catch(err => [err])
  .then(([err, value]) => callback(err, value));
};
