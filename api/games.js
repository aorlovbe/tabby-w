let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const games              = 'platform:games';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');
const accelera = require("../services/producer");
const send = require("@polka/send-type");
const requestIp = require("request-ip");

class Games {
    static createApiKey(req, callback) {
        let api_key = nanoid.getmax();
        if (req.body.game_id !== undefined && req.body.responsible !== undefined) {
            redis.hset('platform:api', api_key, JSON.stringify({
                "timestamp": Math.floor(new Date()),
                "system": req.body.game_id,
                "responsible": req.body.responsible,
                "registration_date": moment(new Date()).format('YYYY-MM-DD'),
                "registration_time": moment(new Date()).format('HH:mm'),
                "registration_datetime": moment(new Date()).format('YYYY-MM-DD HH:mm:ss'),
            }), function (err, result) {
                if (err) {
                    console.log('Error while creating API key:', err);
                    callback(true, null)
                } else {
                    console.log('Created for:', req.body.game_id, api_key);
                    callback(false, api_key)
                }
            });
        } else {
            console.log('Error while creating API key: parameters not set');
            callback(true, null)
        }
    }

    static deleteApiKey(req, callback) {
        if (req.body.key !== undefined) {
            redis.hdel('platform:api', req.body.key, function (err, result) {
                if (err) {
                    console.log('Error while deleting API key:', err);
                    callback(true, null)
                } else {
                    console.log('Deleted key:', req.body.key);
                    callback(false, req.body.key)
                }
            });
        } else {
            console.log('Error while creating API key: parameters not set');
            callback(true, null)
        }
    }

    static list(req, callback) {
        log.info('Getting games');
        redis.hgetall(games, function (err, result) {
            if (err || result === null) {
                log.info('There are no games found');
                return callback(false, {});
            } else {
                log.info('Games found:', _.size(result));
                if (_.size(result) !== 0) {
                    let i = 0;
                    let data = {};
                    _.forEach(result, function (value, key) {
                        _.set(data, key, _.omit(isJSONstring(value),['private']));
                        i++;
                    });

                    if (i === _.size(result)) {
                        return callback(null, data);
                    }
                }
            }
        });
    }

    static find(req, callback) {
        //log.info('Searching games by game ID:', req.body.game_id);
        redis.hget(games, req.body.game_id, function (err, game) {
            if (err || game === null) {
                log.error('Game not found by id:', req.body.game_id);
                return callback(true, {});
            } else {
                return callback(null, _.omit(JSON.parse(game),['private']));
            }

        });
    }

    static findwithprivate(req, callback) {
        //log.info('Searching games by game ID with private section:', req.body.game_id);
        redis.hget(games, req.body.game_id, function (err, game) {
            if (err || game === null) {
                log.error('Game not found by id:', req.body.game_id);
                return callback(true, null);
            } else {
                //log.info('Game found by id:', req.body.game_id);
                return callback(false, JSON.parse(game));
            }

        });
    }

    static check(game_id, callback) {
        //log.info('Searching games by game ID:', game_id);
        redis.hget(games, game_id, function (err, game) {
            if (err || game === null) {
                log.error('Game not found by id:', game_id);
                return callback(true, null);
            } else {
                return callback(null, _.omit(JSON.parse(game),['private']));
            }

        });
    }

    static create(req, callback) {
        log.info('Creating new game:', JSON.stringify(req.body));

        redis.hset(games, req.body.game_id, isJSON(req.body), function (err, result) {
            if (err) {
                log.error('Game cannot be stored:', req.body, err);
                return callback(true);
            } else {
                log.info('Game is created:', result);
                return callback(false, req.body.game_id)
            }
        });
    }

    static remove(req, callback) {
        log.info('Searching game by game ID:', req.body.game_id);
        redis.hdel(games, req.body.game_id, function (err, result) {
            if (err) {
                log.error('Game cannot be deleted:', req.body.game_id, err);
                return callback(true);
            } else {
                log.info('Game is deleted:', result);
                return callback()
            }
        });
    }

    static modify(req, callback) {
        log.info('Searching game by game ID:', req.body.game_id);
        redis.hget(games, req.body.game_id, function (err, result) {
            if (err) {
                log.error('Game cannot be modified:', req.body.game_id, err);
                return callback(true);
            } else {

                if (result !== null) {
                    let game = JSON.parse(result);

                    if (_.size(req.body) !== 0) {
                        let i = 0;
                        _.forEach(req.body, function (value, key) {
                            _.set(game, key, value);
                            i++;
                        });

                        if (i === _.size(req.body)) {
                            redis.hset(games, req.body.game_id, JSON.stringify(game), function() {
                                log.info('Game is updated:', JSON.stringify(game));
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

    static gameSession(req, res, done) {
        req.body.session = nanoid.get_num_various(1,50);
        let data = {
            "timestamp": Math.floor(new Date()),
            "profile_id": req.body.profile_id,
            "session" : req.body.session[0],
            "min" : req.body.game.private.sessions.min,
            "max" : req.body.game.private.sessions.max,
            "level" : req.body.level,
            "reward": req.body.prize,
            "time": moment(new Date()).format('HH:mm'),
            "datetime": moment(new Date()).format('YYYY-MM-DD HH:mm:ss'),
            "datetime_report": moment(new Date()).format('YYYYMMDD_HHmmss') //20220628_120000
        }

        redis.multi()
            .set('platform:games:'+req.body.game.game_id+':sessions:'+req.body.profile_id, JSON.stringify(data))
            .expire('platform:games:'+req.body.game.game_id+':sessions:'+req.body.profile_id, 3000) //50 минут
            .exec(function (err) {
                if (err) {
                    log.error('Session', req.body.session[0], req.body.game.game_id, 'is not created for', req.body.token, req.body.profile_id);
                } else {
                    log.info('Session is created:', req.body.session[0], req.body.profile_id, JSON.stringify(data));
                }
                done();
            });
    }

    static checkSession(req, res, done) {
        redis.multi()
            .get('platform:games:'+req.body.game.game_id+':sessions:'+req.body.profile_id)
            .del('platform:games:'+req.body.game.game_id+':sessions:'+req.body.profile_id)
            .exec(function (err, result) {
                if (err || result[1] === 0) {
                    let ip = requestIp.getClientIp(req).toString();
                    log.error('Session is not active for', req.body.profile_id, ', result is not accepted:', req.body.profile_id, req.body.stars, req.body.level, ip);
                    req.body.session = 'expired';
                    done();
                } else {
                    req.body.session = 'valid';
                    req.body.session_data = JSON.parse(result[0])
                    log.info('Session was active, result accepted:', req.body.profile_id, result[0]);
                    done();
                }
            });
    }

    static lookSession(req, res, done) {
        redis.multi()
            .get('platform:games:'+req.body.game.game_id+':sessions:'+req.body.profile_id)
            .exec(function (err, result) {
                if (err || result[1] === 0) {
                    let ip = requestIp.getClientIp(req).toString();
                    log.error('Session is not active for', req.body.profile_id, ', result is not accepted:', req.body.profile_id, req.body.stars, req.body.level, ip);
                    req.body.session = 'expired';
                    done();
                } else {
                    req.body.session = 'valid';
                    req.body.session_data = JSON.parse(result[0])
                    log.info('Session was active, result accepted:', req.body.profile_id, result[0]);
                    done();
                }
            });
    }

    static storeSession(session, profile_id, game, level, reward, callback) {
        redis.set('platform:games:'+game.game_id+':sessions', session, JSON.stringify({
            "timestamp": Math.floor(new Date()),
            "profile_id": profile_id,
            "min" : game.private.sessions.min,
            "max" : game.private.sessions.max,
            "level" : level,
            "reward": reward,
            "time": moment(new Date()).format('HH:mm'),
            "datetime": moment(new Date()).format('YYYY-MM-DD HH:mm:ss'),
        }), function (err) {
            if (err) {
                log.error('Error while creating session key:', err);
                callback(true)
            } else {
                log.info('Session is stored for:', game.game_id, session, profile_id);
                callback(false)
            }
        });
    }

    static validateSession(session, profile_id, game, result, callback) {
        redis.hget('platform:games:'+game.game_id+':sessions', session, function (err, data) {
            if (err || data === null) {
                log.error('Error while getting session key and validate result:', err, game.game_id, session, profile_id, result, data);
                callback(true)
            } else {
                log.info('Validating session:', game.game_id, session, profile_id, result, data);
                let s_data = JSON.parse(data);
                redis.hdel('platform:games:'+game.game_id+':sessions', session, function (err, results) {
                    if (!err && results === 1 && (parseFloat(result) <= game.private.sessions.max)) {
                        log.info('Session was deleted, increasing counters for:', profile_id, game.game_id, session);
                        callback(false, JSON.parse(data));
                    } else {
                        log.error('Error while validating result:', err, results, result, session, game.private.sessions.max);

                        //Publish fake-data event
                        accelera.publishTrigger(s_data.profile_id, "fake-data", {
                            "profile_id" : s_data.profile_id,
                            "game_id" : game.game_id,
                            "session" : session,
                            "result" : result,
                            "max" : s_data.max,
                            "min" : s_data.min,
                            "level" : s_data.level
                        }).then(function (){
                            log.debug('Trigger was published:', "fake-data");
                        }).catch(e => {
                            log.error('Failed to publish trigger:', e);
                        });

                        callback(true);
                    }
                })
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

module.exports = Games;