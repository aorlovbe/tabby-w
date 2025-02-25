let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const rewards              = 'platform:profile:';
const profiles              = 'platform:games:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');
const momentTimezone = require("moment-timezone");
const send = require("@polka/send-type");
const settings = require("../settings");
const accelera = require("../services/producer");

class Rewards {
    static find(req, callback) {
        log.info('Searching rewards by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, {});
            } else {
                redis.hgetall(rewards + profile + ":rewards", function (err, result) {
                    if (err || result === null) {
                        log.info('There are no rewards found for:', req.user.id, profile);
                        return callback(false, {});
                    } else {
                        log.info('Rewards found:', _.size(result));
                        if (_.size(result) !== 0) {
                            let i = 0;
                            let data = {};
                            _.forEach(result, function (value, key) {
                                value = decodeHTMLEntities(value);
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
        log.info('Searching rewards by profile ID:', req.body.profile_id);
        redis.hgetall(rewards + req.body.profile_id + ":rewards", function (err, result) {
            if (err || result === null) {
                log.debug('There are no rewards found for:', req.body.profile_id);
                return callback(false, {});
            } else {
                log.info('Rewards found:', _.size(result));
                if (_.size(result) !== 0) {
                    let i = 0;
                    let data = {};
                    _.forEach(result, function (value, key) {
                        value = decodeHTMLEntities(value);
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
        log.info('Searching rewards by profile ID:', req.body.profile_id);
        let id = (req.body.unique === 'true') ? nanoid.get() : req.body.id;
        req.body.timestamp = Math.floor(new Date());
        req.body.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
        req.body.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
        req.body.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');

        redis.hset(rewards + req.body.profile_id + ":rewards", id, isJSON(req.body), function (err, result) {
            if (err) {
                log.error('Reward cannot be stored:', req.body.profile_id, req.body, err);
                return callback(true);
            } else {
                log.info('Reward is created:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'created',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    context : isJSON(req.body),
                    name : id.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('rewards', data, function () {});

                return callback(false, id)
            }
        });
    }

    static remove(req, callback) {
        log.info('Searching rewards by profile ID:', req.body.profile_id);
        redis.hdel(rewards + req.body.profile_id + ":rewards", req.body.name, function (err, result) {
            if (err) {
                log.error('Reward cannot be deleted:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Reward is deleted:', result);

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

                Bulk.store('rewards', data, function () {});

                return callback()
            }
        });
    }

    static modify(req, callback) {
        log.info('Searching rewards by profile ID:', req.body.profile_id);

        redis.hget(rewards + req.body.profile_id + ":rewards", req.body.id, function (err, result) {
            if (err) {
                log.error('Reward cannot be modified:', req.body.profile_id, req.body);
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

                            redis.hset(rewards + req.body.profile_id + ":rewards", req.body.id, JSON.stringify(achievement), function() {
                                log.info('Reward is updated:', JSON.stringify(achievement));

                                let data = {
                                    timestamp : Math.floor(new Date()),
                                    profile_id : req.body.profile_id,
                                    status : 'modified',
                                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                    context : JSON.stringify(achievement),
                                    name : req.body.id.toString(),
                                    date : moment(new Date()).format('YYYY-MM-DD'),
                                    time: moment(new Date()).format('HH:mm'),
                                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                };

                                Bulk.store('rewards', data, function () {});


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

    static getMatchPartners(req, res, callback) {
        log.debug('Processing match partners:', req.body.game.game_id, req.body.profile_id);

        Rewards.findbyprofile(req, function (err, issued){
            if (err) return send(res, 200, {"status" : "failed", "partner" : {}});

            let rewards = req.body.game.rewards;

            let partners_id = [];
            let issued_id = Object.keys(issued);

            let partners = rewards.filter((r) => {
                if (r.type === 'partners' && r.status === 'active')
                    partners_id.push(r.id)
                return r;
            })


            let remained = partners_id.filter( function( el ) {
                return issued_id.indexOf( el ) < 0;
            } );

            log.info('Remained partners are:', req.body.profile_id, remained)

            if (remained.length !== 0) {
                let id = remained[Math.floor(Math.random()*remained.length)];
                let gift = rewards.filter((r) => {
                    if (r.id === id) return r;
                });


                //Sending reward to Accelera flow
                //Publish freepack_available event
                accelera.publishTrigger(req.body.profile_id, "partner-reward", {
                    "profile_id" : req.body.profile_id,
                    "game_id" : req.body.game.game_id,
                    "id" : gift[0].id,
                    "type" : gift[0].type,
                    "status" : gift[0].status,
                    "short_description" : gift[0].short_description,
                    "full_description" : gift[0].full_description,
                    "link" : gift[0]["link"]
                }).then(function (){
                    log.debug('Trigger was published:', "partner-reward");
                }).catch(e => {
                    log.error('Failed to publish trigger:', e);
                });

                callback(false, gift[0]);

            } else {
                req.body.partner = {}
                callback(false, {});
            }

        })

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

function decodeHTMLEntities(text) {
    if (typeof text === 'string') {
        let entities = [
            ['#95','_'],
            ['#x3D', '='],
            ['amp', '&'],
            ['apos', '\''],
            ['#x27', '\''],
            ['#x2F', '/'],
            ['#39', '\''],
            ['#47', '/'],
            ['lt', '<'],
            ['gt', '>'],
            ['nbsp', ' '],
            ['quot', '"'],
            ['quote', '"'],
            ['#39', "'"],
            ['#34','"']
        ];

        for (let i in entities) {
            let toreplace = '&'+entities[i][0]+';';
            text = text.replace(new RegExp(toreplace, 'g'), entities[i][1])

        }

        return text;
    } else {
        return text;
    }
}



module.exports = Rewards;