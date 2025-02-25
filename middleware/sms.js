const producer = require('../services/producer');
const axios = require('axios');
const _ = require('lodash');
const settings = require('../settings');
const qs = require('qs');
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const Bulk = require("../api/bulk");
const log = require('../services/bunyan').log;
let redis = require('../services/redis').redisclient;

class SMS {
    static send(data, callback) {
        let _data = _.cloneDeep(data);

        decodeHTMLEntities(_data["text"], function (updated){
            _data["text"] = updated;
            _data["action"] = "post_sms";
            _data["sender"] = settings.beeline.sender;
            _data["time_period"] = "00:00-00:00";
            _data["time_local"] = "0";
            _data["autotrimtext"] = "false";
            _data["period"] = "600";
            _data["user"] = settings.beeline.userid;
            _data["pass"] = settings.beeline.pass;

            axios({
                method: 'POST',
                url: settings.beeline.sms,
                headers: { 'Content-Type' : 'application/x-www-form-urlencoded' },
                data: qs.stringify(_data),
                timeout: 30000
            }).then(response => {
                log.debug('Processed Beeline SMS request with code', response.status);
                producer.publishTrigger(_data.profile_id, "sent_sms", _data);

                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "sms",
                    page : "",
                    profile_id : data.profile_id.toString(),
                    status : 'sent',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : _data.message,
                    additional : _data.target.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});
                callback();

            }).catch(err => {
                log.error('Failed to process Beeline SMS message', _data, err);

                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "sms",
                    page : "",
                    profile_id : _data.profile_id.toString(),
                    status : 'not-sent',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : _data.message,
                    additional : _data.target.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});

                callback(true);
            });
        })


        function decodeHTMLEntities(text, callback) {
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

                callback(text);
            } else {
                callback(text);
            }
        }
    }

    static sendAllOperators(data, callback) {
        let _data = _.cloneDeep(data);

        decodeHTMLEntities(_data["text"], function (updated){
            _data["text"] = updated;
            _data["action"] = "post_sms";
            _data["sender"] = settings.beeline.sender_all;
            _data["time_period"] = "00:00-00:00";
            _data["time_local"] = "0";
            _data["autotrimtext"] = "false";
            _data["period"] = "600";
            _data["user"] = settings.beeline.userid_all;
            _data["pass"] = settings.beeline.pass_all;

            axios({
                method: 'POST',
                url: settings.beeline.sms,
                headers: { 'Content-Type' : 'application/x-www-form-urlencoded' },
                data: qs.stringify(_data),
                timeout: 30000
            }).then(response => {
                log.debug('Processed Beeline SMS request with code', response.status);
                producer.publishTrigger(_data.profile_id, "sent_sms", _data);

                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "sms",
                    page : "",
                    profile_id : data.profile_id.toString(),
                    status : 'sent',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : _data.message,
                    additional : _data.target.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});
                callback();

            }).catch(err => {
                log.error('Failed to process Beeline SMS message', _data, err);

                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "sms",
                    page : "",
                    profile_id : _data.profile_id.toString(),
                    status : 'not-sent',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : _data.message,
                    additional : _data.target.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});

                callback(true);
            });
        })


        function decodeHTMLEntities(text, callback) {
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

                callback(text);
            } else {
                callback(text);
            }
        }
    }

    static queue(data, callback) {
        let _data = _.cloneDeep(data);

        decodeHTMLEntities(_data["text"], function (updated){
            _data["text"] = updated;
            _data["action"] = "post_sms";
            _data["sender"] = settings.beeline.sender;
            _data["time_period"] = "09:00-20:00";
            _data["time_local"] = "1";
            _data["autotrimtext"] = "false";
            _data["period"] = "600";
            _data["user"] = settings.beeline.userid;
            _data["pass"] = settings.beeline.pass;


            //Queue message for a further process (each second by 10)
            redis.rpush('platform:sms-messages', JSON.stringify(_data), function (err, done){
                let event = {
                    timestamp : Math.floor(new Date()),
                    event: "sms",
                    page : "",
                    profile_id : _data.profile_id.toString(),
                    status : 'queued',
                    game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                    details : _data.message,
                    additional : _data.target.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store(_data.game_id, event, function () {});
                callback();
            })
        })


        function decodeHTMLEntities(text, callback) {
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

                callback(text);
            } else {
                callback(text);
            }
        }
    }
}

setInterval(function (){
    //Getting from queue each second and send
    redis.multi()
        .llen('platform:sms-messages')
        .lpop('platform:sms-messages', 5) // by 2
        .exec(function (err, messages){
        if (messages[1] !== null) {
            log.warn(' [...] Processing queued messages (SMS):', messages[0], messages[1].length);

            for (let i in messages[1]) {
                let _data = JSON.parse(messages[1][i]);
                axios({
                    method: 'POST',
                    url: settings.beeline.sms,
                    headers: { 'Content-Type' : 'application/x-www-form-urlencoded' },
                    data: qs.stringify(_data),
                    timeout: 30000
                }).then(response => {
                    producer.publishTrigger(_data.profile_id, "sent_sms", _data);

                    let event = {
                        timestamp : Math.floor(new Date()),
                        event: "sms",
                        page : "",
                        profile_id : _data.profile_id.toString(),
                        status : 'sent-from-queue',
                        game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                        details : _data.message,
                        additional : _data.target.toString(),
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store(_data.game_id, event, function () {});

                }).catch(err => {
                    log.error('Failed to process Beeline SMS message', _data, err);

                    let event = {
                        timestamp : Math.floor(new Date()),
                        event: "sms",
                        page : "",
                        profile_id : _data.profile_id.toString(),
                        status : 'not-sent-from-queue',
                        game_id : (_data.game_id === undefined) ? "" : _data.game_id,
                        details : _data.message,
                        additional : _data.target.toString(),
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

module.exports = SMS;