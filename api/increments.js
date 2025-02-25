let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const increments             = 'platform:profile:';
const increments_zset       = 'platform:increments';
const profiles              = 'platform:games:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');

class Increments {
    static find(req, callback) {
        log.info('Searching increments by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, {});
            } else {
                redis.hgetall(increments + profile + ":increments", function (err, result) {
                    if (err || result === null) {
                        log.info('There are no increments found for:', req.user.id, profile);
                        return callback(false, {});
                    } else {
                        log.info('Increments found:', _.size(result));
                        if (_.size(result) !== 0) {
                            let i = 0;
                            let data = {};
                            _.forEach(result, function (value, key) {
                                _.set(data, key, isJSONstring(value));
                                i++;
                            })

                            if (i === _.size(result)) {
                                return callback(null, data);
                            }
                        }
                    }
                });
            }

        });
    }

    static findbyprofile(req, callback) {
        log.info('Searching increments by profile ID:', req.body.profile_id);
        redis.hgetall(increments + req.body.profile_id + ":increments", function (err, result) {
            if (err || result === null) {
                log.info('There are no increments found for:',req.body.profile_id);
                return callback(false, {});
            } else {
                log.info('Increments found:', _.size(result));
                if (_.size(result) !== 0) {
                    let i = 0;
                    let data = {};
                    _.forEach(result, function (value, key) {
                        _.set(data, key, isJSONstring(value));
                        i++;
                    })

                    if (i === _.size(result)) {
                        return callback(null, data);
                    }
                }
            }
        });
    }

    static create(req, callback) {
        let task = nanoid.getmax();
        _.set(req.body, "task", task);

        let timestamp = (req.body.timestamp === undefined || req.body.timestamp === "") ? Math.floor(new Date()) : parseInt(req.body.timestamp);
        redis.multi()
            .zadd(increments_zset, timestamp, isJSON(req.body))
            .hset(increments + req.body.profile_id + ":increments", task, isJSON(req.body))
            .exec(function (err) {
            if (err) {
                log.error('Increment cannot be stored:', err, req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Increment is created:', req.body.name, '/ timestamp:', timestamp);

                let data = {
                    timestamp :  Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'created',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    name : req.body.name,
                    task : task,
                    period : req.body.period,
                    counter : req.body.counter,
                    increment : req.body.increment,
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('increments', data, function () {});

                return callback(false, req.body.name)
            }
        });
    }

    static createWithTimestamp(req, callback) {
        let task = nanoid.getmax();
        _.set(req.body, "task", task);

        redis.multi()
            .zadd(increments_zset, req.body.timestamp, isJSON(req.body))
            .hset(increments + req.body.profile_id + ":increments", task, isJSON(req.body))
            .exec(function (err, results) {
                if (err) {
                    log.error('Increment cannot be stored:', err, req.body.profile_id, req.body);
                    return callback(true);
                } else {
                    log.info('Increment is created:', req.body.name,task, results );

                    let data = {
                        timestamp :  Math.floor(new Date()),
                        profile_id : req.body.profile_id,
                        status : 'created_by_batch',
                        game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                        name : req.body.name,
                        task : req.body.task,
                        period : req.body.period,
                        counter : req.body.counter,
                        increment : req.body.increment,
                        date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                        time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                        datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store('increments', data, function () {});

                    return callback(false, req.body.name)
                }
            });
    }

    static remove(req, callback) {
        log.info('Searching increments by profile ID:', req.body.profile_id);
        redis.hget(increments + req.body.profile_id + ":increments", req.body.name, function (err, increment) {
            if (err || increment === null) {
                log.error('Increment cannot be deleted:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Increment is found, will be deleted:', increment);

                redis.multi()
                    .zrem(increments_zset, increment)
                    .hdel(increments + req.body.profile_id + ":increments", req.body.name)
                    .exec(function (err) {
                        if (err) {
                            log.error('Increment cannot be deleted:', err, req.body.profile_id, req.body);
                            return callback(true);
                        } else {
                            log.info('Increment is deleted:', req.body.name);

                            let data = {
                                timestamp :  Math.floor(new Date()),
                                profile_id : req.body.profile_id,
                                status : 'removed',
                                game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                name : req.body.name,
                                date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                                time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                                datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                            };

                            Bulk.store('increments', data, function () {});

                            return callback(false)
                        }
                    });
            }
        });
    }

    static modify(req, callback) {
        log.info('Searching increments by profile ID:', req.body.profile_id);
        redis.hget(increments + req.body.profile_id + ":increments", req.body.task, function (err, result) {
            if (err) {
                log.error('Increment cannot be modified:', req.body.profile_id, req.body);
                return callback(true);
            } else {

                if (result !== null) {
                    let parsed = JSON.parse(result);

                    redis.multi()
                        .zscore(increments_zset, result)
                        .zrem(increments_zset, result)
                        .exec(function (err, done) {
                        if (!err && done[1] === 1) {
                            log.info('Previous increment instruction is deleted from ZSET:', result);
                        } else {
                            log.warn('Previous increment instruction cannot be deleted from ZSET:', err, done);
                        }

                        _.set(req.body, "task", parsed.task);

                        redis.multi()
                            .zadd(increments_zset, parseInt(done[0]), isJSON(req.body))
                            .hset(increments + req.body.profile_id + ":increments", req.body.task, isJSON(req.body))
                            .exec(function (err) {
                                if (err) {
                                    log.error('Increment cannot be stored:', err, req.body.profile_id, req.body);
                                    return callback(true);
                                } else {
                                    log.info('Increment is updated:', req.body.task, done[0]);

                                    let data = {
                                        timestamp :  Math.floor(new Date()),
                                        profile_id : req.body.profile_id,
                                        status : 'modified',
                                        game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                        name : req.body.name,
                                        task : req.body.task,
                                        period : req.body.period,
                                        counter : req.body.counter,
                                        increment : req.body.increment,
                                        date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                                        time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                                        datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                    };

                                    Bulk.store('increments', data, function () {});


                                    return callback(false, req.body.name)
                                }
                            });

                    });
                } else {
                    log.info('Nothing to update');
                    callback();
                }
            }
        });
    }

}

function isJSONstring(value) {
    try {
        JSON.parse(value);
    } catch (e) {
        return value;
    }
    return JSON.parse(value);
}

function isJSON(json) {
    let result = (_.isObject(json) === true) ? JSON.stringify(json) : json;
    return result;
}

module.exports = Increments;