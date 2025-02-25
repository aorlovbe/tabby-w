let redis       = require('../services/redis').redisclient;
let nanoid     = require('../services/nanoid');
var argv = require('minimist')(process.argv.slice(1));
console.log('Creating External API key for Accelera.ai Game API with responsible contact person as email:', argv);
const moment    = require('moment');

// Use to create new account
// node management/create-api-key.js -s Flow -r mk@cubesolutions.ru
let api_key = nanoid.getmax();
if (argv.s !== undefined && argv.r !== undefined) {
    redis.hset('platform:api', api_key, JSON.stringify({
        "timestamp": Math.floor(new Date()),
        "system": argv.s,
        "responsible": argv.r,
        "registration_date": moment(new Date()).format('YYYY-MM-DD'),
        "registration_time": moment(new Date()).format('HH:mm'),
        "registration_datetime": moment(new Date()).format('YYYY-MM-DD HH:mm:ss'),
    }), function (err, result) {
        if (err) {
            console.log('Error while creating API key:', err);
            process.exit(1);
        } else {
            console.log('Created for:', argv.s, api_key);
            process.exit(1);
        }
    });
} else {
    console.log('Error while creating API key: parameters not set',);
}

