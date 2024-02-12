export const cdbHash = function hashKey(/** @type {string} */ key) {
    var hash = 5381,
        length = key.length,
        i;

    for (i = 0; i < length; i++) {
        hash = ((((hash << 5) >>> 0) + hash) ^ key.charCodeAt(i)) >>> 0;
    }

    return hash;
};
