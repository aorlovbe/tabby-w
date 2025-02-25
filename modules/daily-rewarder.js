let _                   = require('lodash');
let moment = require('moment');
const timeZone = require('moment-timezone');
let log = require('../services/bunyan').log;
let Leaderboards = require('../api/leaderboard');
let Game = require('../api/games');
const API = require('../middleware/api');
let redis = require('../services/redis').redisclient_rewarder;
const leaderboard_              = 'platform:leaderboard:';
const producer = require('../services/producer');
const settings = require("../settings");
process.env.TZ = 'Europe/Moscow';
let schedule = '30 59 23 * * *';
//let schedule = '00 41 14 * * *';
var CronJob = require('cron').CronJob;

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer and Game API events consumer */

let job = new CronJob(schedule, function() {
    producer.createProducer(settings.instance).then(function (){
        log.warn('Accelera Game API producer is created for schedule:', settings.instance, schedule, process.env.BROKER_CONNECTION);
        //Starting schedule
        let date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
        //let date = '2023-07-31';
        //let week = '11-46';
        //let month = '2022-11'
        let month = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        let week = moment(timeZone.tz('Europe/Moscow')).format('MM-WW');
        log.info('Daily rewarder is started:', schedule, date, month, week);

        Game.list({}, function (err, games){
            if (Object.keys(games).length !== 0) {
                for (let i in games) {
                    if (games[i].game_id === 'rock-paper-scissors') {
                        Leaderboards.getUnmaskedAllDaily({"body" : {"game_id" : games[i].game_id, "name" : "points"}}, date,function (err, leaderboard) {
                            log.warn('Got daily leaderboard for the game (all day):', games[i].game_id);

                            let daily = leaderboard.find(x => x.category === 'daily');
                            // let monthly = leaderboard.find(x => x.category === 'monthly');
                            // let weekly = leaderboard.find(x => x.category === 'weekly');
                            log.warn('Daily leaderboard size is (all day):', Object.keys(daily.scores).length);

                            let daily_limit = games[i].leaderboard.to_reward.daily;
                            let places = [];
                            for (let j in daily.scores) {
                                log.warn(j);
                                places.push({"place" : Object.keys(daily.scores).indexOf(j)+1, "player_id" : j, "score" : daily.scores[j]})

                                // Processing weekly / monthly updates with best score GT flag (
                                // Only update existing elements if the new score is greater than the current score. This flag doesn't prevent adding new elements.)
                                redis.multi()
                                    .zadd(leaderboard_ + games[i].game_id + ":points:" + week, 'GT', daily.scores[j], j)
                                    .zadd(leaderboard_ + games[i].game_id + ":points:" + month, 'GT', daily.scores[j], j)
                                    .exec(function (err) {
                                        if (err) {
                                            log.error('Monthly / weekly leaderboard cannot be modified by daily worker, profile / name:', daily.scores[j], j, err);
                                        }
                                    });
                            }

                            log.warn('Got places: TOP', daily_limit, places.slice(0, daily_limit))
                            let rewarded = places.slice(0, daily_limit);

                            //Reformatting rewards because they have ranges
                            //Reformat leaderboard daily/weekly/monthly data
                            let daily_reformatted = [];
                            for (let k in games[i].leaderboard.gifts.daily ) {
                                if (typeof ( games[i].leaderboard.gifts.daily[k].place) === 'string') {
                                    reformat(
                                        games[i].leaderboard.gifts.daily[k].place.split('-')[0],
                                        games[i].leaderboard.gifts.daily[k].place.split('-')[1],
                                        games[i].leaderboard.gifts.daily[k])
                                } else {
                                    daily_reformatted.push( games[i].leaderboard.gifts.daily[k])
                                }
                            }

                            function reformat(from, to, data){
                                for (let z = parseInt(from); z <= parseInt(to); z++) {
                                    let newdata= _.cloneDeep(data);
                                    newdata.place = z;
                                    daily_reformatted.push(newdata)
                                }
                            }

                            //Processing events to daily scenario
                            producer.createProducer(settings.instance).then(function (){
                                log.warn('Accelera Game API producer is created:', settings.instance);

                                for (let x in rewarded){
                                    //Pushing to accelera
                                    try {
                                        let reward = daily_reformatted[rewarded[x].place-1].reward;
                                        let context = {
                                            "place" : rewarded[x].place,
                                            "range" : daily["range"],
                                            "score" : parseInt(rewarded[x].score),
                                            "player_id" : rewarded[x].player_id,
                                            "reward" : reward,
                                            "sum" : reward.split('-')[1],
                                            "title" : daily_reformatted[rewarded[x].place-1].title,
                                            "name" : daily_reformatted[rewarded[x].place-1].name,
                                            "details" : daily_reformatted[rewarded[x].place-1].details
                                        };
                                        log.warn('Processing reward trigger:', JSON.stringify(context));

                                        //Publish trigger
                                        //API.publish(rewarded[x].player_id, 'daily_reward', context, function (){})
                                        producer.publishTrigger(rewarded[x].player_id, 'daily_reward', context, function (){}).then(function (){})

                                    } catch (e) {
                                        log.error(e)
                                    }

                                }
                            });

                        })
                    }
                }
            }
        })
    });
});

job.start();