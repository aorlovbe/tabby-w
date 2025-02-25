let _                   = require('lodash');
let moment = require('moment');
const timeZone = require('moment-timezone');
let log = require('../services/bunyan').log;
let redis = require('../services/redis').redisclient_rewarder;
const producer = require('../services/producer');
const settings = require("../settings");
const API = require("../middleware/api");
const Counter = require("../api/counters");
const send = require("@polka/send-type");
process.env.TZ = 'Europe/Moscow';

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer and Game API events consumer */

producer.createProducer(settings.instance).then(function (){
    //Starting schedule
    let date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
    //let date = '2022-06-16';
    let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
    let week = moment(timeZone.tz('Europe/Moscow')).format('MM-WW');
    log.warn('Blacklist clearer is started:', date, month, week);

    redis.smembers('platform:blacklist', function (err, members){
        log.warn('Got members of blacklist', err, members);

        for (let i in members) {

            Counter.findbyprofile({"body" : {"profile_id" : members[i]}}, function (err, counter){
                let context = {
                    "profile_id" : members[i],
                    "game_id" : "rock-paper-scissors",
                    "tries" : (counter.tries !== undefined) ? parseInt(counter.tries) : 0
                };

                log.warn('Processing blacklist trigger:', JSON.stringify(context));

                //Publish trigger
                API.publish(members[i], 'clear-blacklist', context, function (){})

            })

        }
    })

});