const producer = require('../services/producer');
const axios = require('axios');
const _ = require('lodash');
const settings = require('../settings');
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const Bulk = require("../api/bulk");
const Game = require("../api/games");
const log = require('../services/bunyan').log;
let redis = require('../services/redis').redisclient;
const sha = require("../services/sha");

class PUSH {
    static send(data, callback) {
        let _data = _.cloneDeep(data);
        Game.findwithprivate({"body" : {"game_id" : _data.game_id}}, function (err, games) {
            let xapi = sha.encrypt(_data.code.toString() + _data.ctn.toString() + _data.messageKey.toString() + games.private.salt);
            _data["x-api-key"] = xapi;
            log.warn('Push:', games.private.salt, xapi)

            axios({
                method: 'POST',
                url: settings.beeline.push,
                headers: {
                    'x-api-key' : xapi
                },
                data: {
                    "code": _data.code,
                    "messageKey": _data.messageKey,
                    "ctn": _data.ctn.toString()
                },
                timeout: 30000
            }).then(response => {
                log.debug('Processed Beeline PUSH request with code', response.status);
                producer.publishTrigger(_data.profile_id, "sent_push", _data);

                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "push",
                    page : "",
                    profile_id : _data.profile_id.toString(),
                    status : 'sent',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : JSON.stringify(_data),
                    additional : _data.ctn.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});
                callback();

            }).catch(err => {
                log.error('Failed to process Beeline PUSH message', _data, err);

                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "push",
                    page : "",
                    profile_id : _data.profile_id.toString(),
                    status : 'not-sent',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : JSON.stringify(_data),
                    additional : _data.ctn.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});

                callback(true);
            });
        })
    }

    static queue(data, callback) {
        let _data = _.cloneDeep(data);
        Game.findwithprivate({"body" : {"game_id" : _data.game_id}}, function (games) {
            _data["x-api-key"] = sha.encrypt(_data.code.toString() + _data.ctn.toString() + _data.messageKey.toString() + games.private.salt);
            //Queue message for a further process (each second by 10)
            redis.rpush('platform:push-messages', JSON.stringify(_data), function (err, done){
                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "push",
                    page : "",
                    profile_id : _data.profile_id.toString(),
                    status : 'queued',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : JSON.stringify(_data),
                    additional : _data.ctn.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});
                callback();
            })
        });

    }
}

setInterval(function (){
    //Getting from queue each second and send
    redis.multi()
        .llen('platform:push-messages')
        .lpop('platform:push-messages', 50) // by 50
        .exec(function (err, messages){
        if (messages[1] !== null) {
            log.warn(' [...] Processing queued messages (PUSH):', messages[0], messages[1].length);

            for (let i in messages[1]) {
                let _data = JSON.parse(messages[1][i]);

                axios({
                    method: 'POST',
                    url: settings.beeline.push,
                    headers: {
                        'x-api-key' : _data["x-api-key"]
                    },
                    data: {
                        "code": _data.code,
                        "messageKey": _data.messageKey,
                        "ctn": _data.ctn.toString()
                    },
                    timeout: 30000
                }).then(response => {
                    producer.publishTrigger(_data.profile_id, "sent_push", _data);

                    let event = {
                        timestamp : Math.floor(new Date()),
                        event: "push",
                        page : "",
                        profile_id : _data.profile_id.toString(),
                        status : 'sent-from-queue',
                        game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                        details : JSON.stringify(_data),
                        additional : _data.ctn.toString(),
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(_data.game_id, event, function () {});

                }).catch(err => {
                    log.error('Failed to process Beeline Push message', _data, err);

                    let event = {
                        timestamp : Math.floor(new Date()),
                        event: "push",
                        page : "",
                        profile_id : _data.profile_id.toString(),
                        status : 'not-sent-from-queue',
                        game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                        details : JSON.stringify(_data),
                        additional : _data.ctn.toString(),
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(_data.game_id, event, function () {});

                });
            }
        }
    })
}, 10000)

module.exports = PUSH;