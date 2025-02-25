let jws = require("jws");
let algorithm = 'HS256';
const settings = require("../settings");

var encrypt = function encrypt(text)
{
    const signature = jws.sign({
        header: { alg: algorithm },
        payload: text,
        secret: settings.jwt
    });
    return signature.toString();
};

var decrypt = function decrypt(text)
{
    try {
        let decoded = jws.decode(text);
        return JSON.parse(decoded.payload);
    } catch (ex) {
        console.log('Decryption failed:', ex);
        return false;
    }
};

var verify = function verify(signature)
{
    try {
        let verified = jws.verify(signature, algorithm, settings.jwt);
        return verified;
    } catch (ex) {
        console.log('Verification failed:', ex);
        return false;
    }
};

exports.encrypt = encrypt;
exports.decrypt = decrypt;
exports.verify = verify;