var redis = require("redis");
const settings = require('../settings');
const log = require('./bunyan').log;
const bluebird = require('bluebird');

bluebird.promisifyAll(redis.RedisClient.prototype);
bluebird.promisifyAll(redis.Multi.prototype);

var redisclient = redis.createClient(settings.redis.connection);
var redisclient_bulk = redis.createClient(settings.redis.connection);
var redisclient_rewarder = redis.createClient(settings.redis.connection);

redisclient.on("error", function (err) {
    log.error('Operational DB exception:', err);
});

redisclient.on('connect', function() {
    log.info('Connected to operational instance');
});

redisclient.on('end', function() {
    log.warn('Established operational connection has been closed');
});

redisclient.on('ready', function() {
    log.info('Operational DB is ready');
});

exports.redisclient = redisclient;
exports.redisclient_bulk = redisclient_bulk;
exports.redisclient_rewarder = redisclient_rewarder;