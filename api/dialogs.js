let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const dialogs              = 'platform:profile:';
const profiles              = 'platform:games:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');

class Dialogs {
    static find(req, callback) {
        log.info('Searching dialogs by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, {});
            } else {
                redis.hgetall(dialogs + profile + ":dialogs", function (err, result) {
                    if (err || result === null) {
                        log.info('There are no dialogs found for:', req.user.id, profile);
                        return callback(false, {});
                    } else {
                        log.info('Dialogs found:', _.size(result));
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
        log.info('Searching dialogs by profile ID:', req.body.profile_id);
        redis.hgetall(dialogs + req.body.profile_id + ":dialogs", function (err, result) {
            if (err || result === null) {
                log.info('There are no dialogs found for:', req.body.profile_id);
                return callback(false, {});
            } else {
                log.info('Dialogs found:', _.size(result));
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
        log.info('Searching dialogs by profile ID:', req.body.profile_id);
        let id = req.body.name;
        req.body.timestamp = Math.floor(new Date());

        redis.hset(dialogs + req.body.profile_id + ":dialogs", req.body.name, isJSON(req.body), function (err, result) {
            if (err) {
                log.error('Dialog cannot be stored:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Dialog is created:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'created',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    context : isJSON(req.body),
                    name : req.body.name.toString(),
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('dialogs', data, function () {});

                return callback(false, req.body.name)
            }
        });
    }

    static remove(req, callback) {
        log.info('Searching dialogs by profile ID:', req.body.profile_id);
        redis.hdel(dialogs + req.body.profile_id + ":dialogs", req.body.name, function (err, result) {
            if (err) {
                log.error('Dialog cannot be deleted:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Dialog is deleted:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'removed',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    context : isJSON(req.body),
                    name : req.body.name.toString(),
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('dialogs', data, function () {});
                return callback()
            }
        });
    }

    static modify(req, callback) {
        log.info('Searching dialogs by profile ID:', req.body.profile_id);
        redis.hget(dialogs + req.body.profile_id + ":dialogs", req.body.name, function (err, result) {
            if (err) {
                log.error('Dialog cannot be modified:', req.body.profile_id, req.body);
                return callback(true);
            } else {

                if (result !== null) {
                    let dialog = JSON.parse(result);

                    if (_.size(req.body) !== 0) {
                        let i = 0;
                        _.forEach(req.body, function (value, key) {
                            _.set(dialog, key, value);
                            i++;
                        });

                        if (i === _.size(req.body)) {
                            redis.hset(dialogs + req.body.profile_id + ":dialogs", req.body.name, JSON.stringify(dialog), function() {
                                log.info('Dialog is updated:', JSON.stringify(dialog));

                                let data = {
                                    timestamp : Math.floor(new Date()),
                                    profile_id : req.body.profile_id,
                                    status : 'modified',
                                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                    context : JSON.stringify(dialog),
                                    name : req.body.name.toString(),
                                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                };

                                Bulk.store('dialogs', data, function () {});


                                callback();
                            });
                        }
                    } else {
                        log.info('Nothing to update');
                        callback();
                    }
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

module.exports = Dialogs;