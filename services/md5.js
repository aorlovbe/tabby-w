/**
 * Created by fla_ on 27.03.17.
 */
var crypto      = require('crypto');

exports.md5 = function(data) {
    var md5 = crypto.createHash('md5').update(data.toString()).digest("hex");
    return md5.toString();
};

