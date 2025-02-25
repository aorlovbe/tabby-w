let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const leaderboard              = 'platform:leaderboard:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');
const momentTimezone = require("moment-timezone");

class Leaderboard {

    static get(req, callback) {
        let date = moment(timeZone.tz('Europe/Moscow')).format('MM-DD-YYYY');
        let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        let week = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-WW');
        let total = 'TOTAL';
        let profile_id = (req.body.player_id === undefined) ? '' : req.body.player_id;

        //Getting top 10
        redis.multi()
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + date, 0, 99, "WITHSCORES")
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + month, 0, 99, "WITHSCORES")
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + week, 0, 149, "WITHSCORES")
            .ZREVRANK(leaderboard + req.body.game_id + ":" + req.body.name + ":" + date, profile_id)
            .ZREVRANK(leaderboard + req.body.game_id + ":" + req.body.name + ":" + month, profile_id)
            .ZREVRANK(leaderboard + req.body.game_id + ":" + req.body.name + ":" + week, profile_id)
            .ZSCORE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + date, profile_id)
            .ZSCORE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + month, profile_id)
            .ZSCORE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + week, profile_id)
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + total, 0, 249, "WITHSCORES")
            .ZREVRANK(leaderboard + req.body.game_id + ":" + req.body.name + ":" + total, profile_id)
            .ZSCORE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + total, profile_id)
            .exec(function (err, ratings) {
            if (err) {
                log.error('Leaderboard cannot be selected:', req.body.game_id, req.body.name, err);
                return callback(true, [{},{},{},{}]);
            }

            if (_.size(ratings) !== 0) {
                let ratings_date = _.fromPairs(_.chunk(ratings[0], 2));
                let ratings_month = _.fromPairs(_.chunk(ratings[1], 2));
                let ratings_week = _.fromPairs(_.chunk(ratings[2], 2));

                //let ratings_total = _.fromPairs(_.chunk(ratings[9], 2));

                let score_day = (ratings[6] === null) ? 0 : ratings[6];
                let score_month = (ratings[7] === null) ? 0 : ratings[7];
                let score_week = (ratings[8] === null) ? 0 : ratings[8];

                let masked_num1 = '+7***'+profile_id.substr(4,1).toString();
                let masked_num2 = '*'+profile_id.substr(6,1).toString();
                let masked_num3 = profile_id.substr(7).toString();
                let masked_num = masked_num1+masked_num2+masked_num3;
                //let masked_num = '+7*****'+profile_id.substr(6).toString();

                //let score_total = (ratings[11] === null) ? 0 : ratings[11];

                //Masked numbers
                callback(null,[
                    {"range" : date, "category" : "daily", "scores" : mask(ratings_date), "position" : ((ratings[3] === null) ? 0 : (ratings[3]+1)), "score": score_day, "player" : masked_num},
                    {"range" : month, "category" : "monthly", "scores" : mask(ratings_month),"position" : ((ratings[4] === null) ? 0 : (ratings[4]+1)), "score": score_month, "player" : masked_num},
                    {"range" : week, "category" : "weekly", "scores" : mask(ratings_week), "position" : ((ratings[5] === null) ? 0 : (ratings[5]+1)), "score": score_week, "player" : masked_num},
                    /*{"range" : total, "category" : "total", "scores" : mask(ratings_total), "position" : ((ratings[10] === null) ? 0 : (ratings[10]+1)), "score": score_total, "player" : masked_num}*/
                ]);
            } else {
                callback(null, [{},{},{},{}])
            }
        })

        function mask(json){
            let _json = {};
            let keys = Object.keys(json);
            let values = Object.values(json);

            for (let i in keys) {
                //let maskedkey = '+7*****'+keys[i].substr(6).toString();
                let masked_num1 = '+7***'+keys[i].substr(4,1).toString();
                let masked_num2 = '*'+keys[i].substr(6,1).toString();
                let masked_num3 = keys[i].substr(7).toString();
                let maskedkey = masked_num1+masked_num2+masked_num3;
                //+79651635198
                //+7***1*65198
                _json[maskedkey] = values[i];
            }

            return _json;
        }
    }

    static getUnmasked(req, callback) {
        let date = moment(timeZone.tz('Europe/Moscow')).format('MM-DD-YYYY');
        let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        let week = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-WW');

        //Getting top 100
        redis.multi()
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + date, 0, 99, "WITHSCORES")
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + month, 0, 99, "WITHSCORES")
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + week, 0, 99, "WITHSCORES")
            .exec(function (err, ratings) {
                if (err) {
                    log.error('Leaderboard cannot be selected:', req.body.game_id, req.body.name, err);
                    return callback(true, [{},{},{}]);
                }

                if (_.size(ratings) !== 0) {
                    let ratings_date = _.fromPairs(_.chunk(ratings[0], 2));
                    let ratings_month = _.fromPairs(_.chunk(ratings[1], 2));
                    let ratings_week = _.fromPairs(_.chunk(ratings[2], 2));


                    //Masked numbers
                    callback(null,[
                        {"range" : date, "category" : "daily", "scores" : ratings_date},
                        {"range" : month, "category" : "monthly", "scores" : ratings_month},
                        {"range" : week, "category" : "weekly", "scores" : ratings_week}]);
                } else {
                    callback(null, [{},{},{}])
                }
            })
    }

    static getUnmaskedAllDaily(req, date, callback) {
        //Getting all players
        redis.multi()
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + date, 0, 100, "WITHSCORES")
            .exec(function (err, ratings) {
                if (err) {
                    log.error('Leaderboard cannot be selected:', req.body.game_id, req.body.name, err);
                    return callback(true, [{},{},{}]);
                }

                if (_.size(ratings) !== 0) {
                    let ratings_date = _.fromPairs(_.chunk(ratings[0], 2));

                    //Masked numbers
                    callback(null,[
                        {"range" : date, "category" : "daily", "scores" : ratings_date}]);
                } else {
                    callback(null, [{},{},{}])
                }
            })
    }

    static getUnmaskedTotal(req, date, callback) {
        //Getting all players
        redis.multi()
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":TOTAL", 0, 249, "WITHSCORES")
            .exec(function (err, ratings) {
                if (err) {
                    log.error('Leaderboard cannot be selected:', req.body.game_id, req.body.name, err);
                    return callback(true, [{},{},{}]);
                }

                if (_.size(ratings) !== 0) {
                    let ratings_date = _.fromPairs(_.chunk(ratings[0], 2));

                    //Masked numbers
                    callback(null,[
                        {"range" : date, "category" : "total", "scores" : ratings_date}]);
                } else {
                    callback(null, [{},{},{}])
                }
            })
    }

    static getUnmaskedbyDate(req, date, month, week, callback) {
        //Getting top 100
        redis.multi()
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + date, 0, 99, "WITHSCORES")
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + month, 0, 150, "WITHSCORES")
            .ZREVRANGE(leaderboard + req.body.game_id + ":" + req.body.name + ":" + week, 0, 150, "WITHSCORES")
            .exec(function (err, ratings) {
                if (err) {
                    log.error('Leaderboard cannot be selected:', req.body.game_id, req.body.name, err);
                    return callback(true, [{},{},{}]);
                }

                if (_.size(ratings) !== 0) {
                    let ratings_date = _.fromPairs(_.chunk(ratings[0], 2));
                    let ratings_month = _.fromPairs(_.chunk(ratings[1], 2));
                    let ratings_week = _.fromPairs(_.chunk(ratings[2], 2));


                    //Masked numbers
                    callback(null,[
                        {"range" : date, "category" : "daily", "scores" : ratings_date},
                        {"range" : month, "category" : "monthly", "scores" : ratings_month},
                        {"range" : week, "category" : "weekly", "scores" : ratings_week}]);
                } else {
                    callback(null, [{},{},{}])
                }
            })
    }

    static remove(req, callback) {
        log.info('Searching dialogs by profile ID:', req.body.profile_id);
        redis.hdel(dialogs + req.body.profile_id + ":dialogs", req.body.name, function (err, result) {
            if (err) {
                log.error('Dialog cannot be deleted:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Dialog is deleted:', result);
                return callback()
            }
        });
    }

    static modify(req, callback) {
        let date = moment(timeZone.tz('Europe/Moscow')).format('MM-DD-YYYY');
        let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        let week = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-WW');
        log.info('Modifying leaderboard by profile ID:', req.body.profile_id, date, month, week);

        //Leaderboard will take best daily score for weekly and monthly score (not simple increment)
        redis.multi()
            .zincrby(leaderboard + req.body.system + ":" + req.body.name + ":" + date, req.body.value, req.body.profile_id)
            .exec(function (err, result) {
            if (err) {
                log.error('Leaderboard cannot be modified, profile / name:', req.body.profile_id, req.body.name);
                return callback(true);
            } else {
                log.info('Daily leaderboard by profile ID was modified:', req.body.profile_id, result[0]);


                let data = {
                    timestamp : Math.floor(new Date()),
                    event: "accelera-api",
                    page : "leaderboard-daily",
                    profile_id : req.body.profile_id,
                    status : 'modified',
                    game_id : (req.body.system === undefined) ? "" : req.body.system,
                    details : req.body.name.toString(),
                    gifts : [req.body.value.toString(), result[0].toString()],
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(req.body.system, data, function () {});

                callback();
            }
        });
    }

    static set(req, callback) {
        let date = moment(timeZone.tz('Europe/Moscow')).format('MM-DD-YYYY');
        let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        let week = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-WW');
        let all = 'TOTAL';
        log.info('Modifying leaderboard by profile ID:', req.body.profile_id, date, month, week);

        redis.multi()
            .zadd(leaderboard + req.body.system + ":" + req.body.name + ":" + date, req.body.value, req.body.profile_id)
            .zadd(leaderboard + req.body.system + ":" + req.body.name + ":" + month, req.body.value, req.body.profile_id)
            .zadd(leaderboard + req.body.system + ":" + req.body.name + ":" + week, req.body.value, req.body.profile_id)
            .zadd(leaderboard + req.body.system + ":" + req.body.name + ":" + all, req.body.value, req.body.profile_id)
            .exec(function (err, result) {
                if (err) {
                    log.error('Leaderboard cannot be modified, profile / name:', req.body.profile_id, req.body.name);
                    return callback(true);
                } else {
                    log.info('Leaderboard by profile ID was set to:', req.body.value, req.body.profile_id);

                    let data_day = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-daily",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[0].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_day, function () {});

                    let data_week = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-weekly",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[2].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_week, function () {});

                    let data_month = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-monthly",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[1].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_month, function () {});

                    let data_all = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-all",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[3].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_all, function () {});


                    callback();
                }
            });
    }

    static increase(req, callback) {
        let date = moment(timeZone.tz('Europe/Moscow')).format('MM-DD-YYYY');
        let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        let week = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-WW');
        let all = 'TOTAL';
        log.info('Modifying leaderboard by profile ID:', req.body.profile_id, date, month, week);

        redis.multi()
            .zincrby(leaderboard + req.body.system + ":" + req.body.name + ":" + date, req.body.value, req.body.profile_id)
            .zincrby(leaderboard + req.body.system + ":" + req.body.name + ":" + month, req.body.value, req.body.profile_id)
            .zincrby(leaderboard + req.body.system + ":" + req.body.name + ":" + week, req.body.value, req.body.profile_id)
            .zincrby(leaderboard + req.body.system + ":" + req.body.name + ":" + all, req.body.value, req.body.profile_id)
            .exec(function (err, result) {
                if (err) {
                    log.error('Leaderboard cannot be modified, profile / name:', req.body.profile_id, req.body.name);
                    return callback(true);
                } else {
                    log.info('Leaderboard by profile ID was set to:', req.body.value, req.body.profile_id);

                    let data_day = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-daily",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[0].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_day, function () {});

                    let data_week = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-weekly",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[2].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_week, function () {});

                    let data_month = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-monthly",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[1].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_month, function () {});

                    let data_all = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-all",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[3].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data_all, function () {});


                    callback();
                }
            });
    }

    static setDaily(req, callback) {
        let date = moment(timeZone.tz('Europe/Moscow')).format('MM-DD-YYYY');
        log.info('Modifying daily leaderboard by profile ID:', req.body.profile_id, date);

        redis.multi()
            .zadd(leaderboard + req.body.system + ":" + req.body.name + ":" + date, req.body.value, req.body.profile_id)
            .exec(function (err, result) {
                if (err) {
                    log.error('Leaderboard cannot be modified, profile / name:', req.body.profile_id, req.body.name);
                    return callback(true);
                } else {
                    log.info('Daily leaderboard by profile ID was set to:', req.body.value, req.body.profile_id);
                    let data = {
                        timestamp : Math.floor(new Date()),
                        event: "accelera-api",
                        page : "leaderboard-daily",
                        profile_id : req.body.profile_id,
                        status : 'modified',
                        game_id : (req.body.system === undefined) ? "" : req.body.system,
                        details : req.body.name.toString(),
                        gifts : [req.body.value.toString(), result[0].toString()],
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(req.body.system, data, function () {});

                    callback();
                }
            });
    }

}

module.exports = Leaderboard;