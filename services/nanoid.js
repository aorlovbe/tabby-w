const nanoid           = require('nanoid');
let log = require('./bunyan').log;

exports.get = function() {
    return nanoid.nanoid(20).toString();
};

exports.getmax = function() {
    return nanoid.nanoid(20*2).toString();
};

exports.get_num = function(num) {

    let array = [];

    for (let i = 1;i<=num;i++) {
        let id = nanoid.nanoid(20).toString();
        array.push(id);

        if (i===num) {
            return array;
        }
    }
};

exports.get_num_various = function(num, length) {

    let array = [];

    for (let i = 1;i<=num;i++) {
        let id = nanoid.nanoid(length).toString();
        array.push(id);

        if (i===num) {
            return array;
        }
    }
};