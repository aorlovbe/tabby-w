let SHA1 = require("crypto-js/sha1");

function encrypt(data) {
    return SHA1(data).toString();
}

exports.encrypt = encrypt;