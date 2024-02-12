export const cdbHash = function hashKey(key: string): number {
    let hash = 5381;
    const length = key.length;
    let i: number;

    for (i = 0; i < length; i++) {
        hash = ((((hash << 5) >>> 0) + hash) ^ key.charCodeAt(i)) >>> 0;
    }

    return hash;
};
