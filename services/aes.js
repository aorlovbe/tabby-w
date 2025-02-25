let key = 'U2FsdGVkX19p1qPQOb01CKKmwikeHW+DnT6F0jkA70cUAlLK3GjOJpZPi9FPN57h';
var AES                 = require("crypto-js/aes");
var CryptoJS            = require("crypto-js");

var encrypt = function encrypt(text)
{
    var ciphertext = AES.encrypt(text, key);
    return ciphertext.toString();
};

var decrypt = function decrypt(text)
{
    try {
        var bytes  = AES.decrypt(text.toString(), key);
        var plaintext = bytes.toString(CryptoJS.enc.Utf8);
        return plaintext;
    } catch (ex) {
        console.log('Decryption failed');
        return false;
    }
};

exports.encrypt = encrypt;
exports.decrypt = decrypt;