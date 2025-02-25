let _                   = require('lodash');
let moment = require('moment');
const timeZone = require('moment-timezone');
let log = require('../services/bunyan').log;
let Leaderboards = require('../api/leaderboard');
let Game = require('../api/games');
const API = require('../middleware/api');

const producer = require('../services/producer');
const settings = require("../settings");
process.env.TZ = 'Europe/Moscow';
//Each Monday at 00:01
let schedule = '00 12 23 * * *';
//let schedule = '55 59 23 * * 0';
var CronJob = require('cron').CronJob;

/* ------------------------------------------------------------- */

let job = new CronJob(schedule, function() {
    /* Accelera Flows triggers producer and Game API events consumer */
    producer.createProducer(settings.instance).then(function (){
        log.info('Accelera Game API producer is created for schedule:', settings.instance, schedule);
        //Starting schedule
        //Get this week on Sunday 23:59:55
        //let lastdate = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
        //let lastmonth = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM');
        //let lastweek = moment(timeZone.tz('Europe/Moscow')).format('MM-WW');

        let lastdate = '2023-02-01';
        let lastmonth = '2023-01';
        let lastweek = '01-04';
        log.warn('Total rewarder is started (last week):', schedule, lastdate, lastmonth, lastweek);

        Game.list({}, function (err, games){
            if (Object.keys(games).length !== 0) {
                for (let i in games) {
                    if (games[i].game_id === 'xmas') {
                        Leaderboards.getUnmaskedTotal({"body" : {"game_id" : games[i].game_id, "name" : "points"}}, lastdate,function (err, leaderboard) {
                            log.info('Got leaderboard for the game:', lastdate, lastmonth, lastweek, games[i].game_id, leaderboard);

                            //let daily = leaderboard.find(x => x.category === 'daily');
                            // let monthly = leaderboard.find(x => x.category === 'monthly');

                            //TOTAL here
                            let weekly = leaderboard.find(x => x.category === 'total');

                            let weekly_limit = games[i].leaderboard.to_reward.monthly;
                            let places = [];
                            for (let j in weekly.scores) {
                                places.push({"place" : Object.keys(weekly.scores).indexOf(j)+1, "player_id" : j, "score" : weekly.scores[j]})
                            }

                            log.warn('Got places TOTAL: TOP', weekly_limit, places.slice(0, weekly_limit))
                            let rewarded = places.slice(0, weekly_limit);

                            //Reformat leaderboard daily/weekly/monthly data
                            let weekly_reformatted = [];
                            for (let k in games[i].leaderboard.gifts.total ) {
                                if (typeof ( games[i].leaderboard.gifts.total[k].place) === 'string') {
                                    reformat_weekly(
                                        games[i].leaderboard.gifts.total[k].place.split('-')[0],
                                        games[i].leaderboard.gifts.total[k].place.split('-')[1],
                                        games[i].leaderboard.gifts.total[k])
                                } else {
                                    weekly_reformatted.push( games[i].leaderboard.gifts.total[k])
                                }
                            }

                            function reformat_weekly(from, to, data){
                                for (let z = parseInt(from); z <= parseInt(to); z++) {
                                    let newdata= _.cloneDeep(data);
                                    newdata.place = z;
                                    weekly_reformatted.push(newdata)
                                }
                            }

                            //Processing events to weekly scenario
                            producer.createProducer(settings.instance).then(function (){
                                log.warn('Accelera Game API producer is created:', settings.instance);
                                log.warn(weekly_reformatted);
                                for (let x in rewarded){
                                    //Pushing to accelera
                                    try {
                                        let reward = weekly_reformatted[rewarded[x].place-1].id;
                                        let personalized = games[i].rewards.find(item => item.id === reward);
                                        let context = {
                                            "place" : rewarded[x].place,
                                            "range" : weekly["range"],
                                            "score" : parseInt(rewarded[x].score),
                                            "player_id" : rewarded[x].player_id,
                                            "id" : personalized.id,
                                            "reward" : personalized,
                                            "title" : weekly_reformatted[rewarded[x].place-1].title,
                                            "name" : weekly_reformatted[rewarded[x].place-1].name,
                                            "details" : weekly_reformatted[rewarded[x].place-1].details
                                        };
                                        log.warn('Processing reward trigger:', JSON.stringify(context));

                                        //Publish trigger
                                        producer.publishTrigger(rewarded[x].player_id, 'total_reward_xmas', context, function (){}).then(function (){})


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