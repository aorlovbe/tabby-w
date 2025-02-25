let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const achievements              = 'platform:profile:';
const profiles              = 'platform:games:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');
const momentTimezone = require("moment-timezone");

class Achievements {
    static find(req, callback) {
        log.info('Searching achievements by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, {});
            } else {
                redis.hgetall(achievements + profile + ":achievements", function (err, result) {
                    if (err || result === null) {
                        log.info('There are no achievements found for:', req.user.id, profile);
                        return callback(false, {});
                    } else {
                        log.info('Achievements found:', _.size(result));
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
        log.info('Searching achievements by profile ID:', req.body.profile_id);
        redis.hgetall(achievements + req.body.profile_id + ":achievements", function (err, result) {
            if (err || result === null) {
                log.debug('There are no achievements found for:', req.body.profile_id);
                return callback(false, {});
            } else {
                log.info('Achievements found:', _.size(result));
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
        log.info('Searching achievements by profile ID:', req.body.profile_id);
        let id = (req.body.unique === 'true') ? nanoid.get() : req.body.name;
        req.body.timestamp = Math.floor(new Date());
        req.body.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
        req.body.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
        req.body.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');

        redis.hset(achievements + req.body.profile_id + ":achievements", id, isJSON(req.body), function (err, result) {
            if (err) {
                log.error('Achievement cannot be stored:', req.body.profile_id, req.body, err);
                return callback(true);
            } else {
                log.info('Achievement is created:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    event: "accelera-api",
                    page : "achievements",
                    profile_id : req.body.profile_id,
                    status : 'created',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    details : id.toString(),
                    additional : isJSON(req.body),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(req.body.game_id, data, function () {});

                return callback(false, req.body.name)
            }
        });
    }

    static remove(req, callback) {
        log.info('Searching achievements by profile ID:', req.body.profile_id);
        redis.hdel(achievements + req.body.profile_id + ":achievements", req.body.name, function (err, result) {
            if (err) {
                log.error('Achievement cannot be deleted:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Achievement is deleted:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'removed',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    name : req.body.name.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('achievements', data, function () {});

                return callback()
            }
        });
    }

    static modify(req, callback) {
        log.info('Searching achievements by profile ID:', req.body.profile_id);

        redis.hget(achievements + req.body.profile_id + ":achievements", req.body.name, function (err, result) {
            if (err) {
                log.error('Achievement cannot be modified:', req.body.profile_id, req.body);
                return callback(true);
            } else {

                if (result !== null) {
                    let achievement = JSON.parse(result);

                    if (_.size(req.body) !== 0) {
                        let i = 0;
                        _.forEach(req.body, function (value, key) {
                            _.set(achievement, key, value);
                            i++;
                        });

                        if (i === _.size(req.body)) {
                            achievement.timestamp = Math.floor(new Date());
                            achievement.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
                            achievement.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
                            achievement.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');

                            redis.hset(achievements + req.body.profile_id + ":achievements", req.body.name, JSON.stringify(achievement), function() {
                                log.info('Achievement is updated:', JSON.stringify(achievement));

                                let data = {
                                    timestamp : Math.floor(new Date()),
                                    event: "accelera-api",
                                    page : "achievements",
                                    profile_id : req.body.profile_id,
                                    status : 'modified',
                                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                    details : req.body.name.toString(),
                                    additional : JSON.stringify(achievement),
                                    date : moment(new Date()).format('YYYY-MM-DD'),
                                    time: moment(new Date()).format('HH:mm'),
                                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                };

                                Bulk.store(req.body.game_id, data, function () {});

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

module.exports = Achievements;