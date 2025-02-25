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
let schedule = '00 10 11 * * *';
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
        //add comm nwmtod

        let lastdate = '2023-10-23';
        let lastmonth = '2023-10';
        let lastweek = '2024-12-49';

        log.warn('Weekly rewarder is started (last week):', schedule, lastdate, lastmonth, lastweek);

        Game.list({}, function (err, games){
            if (Object.keys(games).length !== 0) {
                for (let i in games) {
                    if (games[i].game_id === 'rock-paper-scissors') {
                        Leaderboards.getUnmaskedbyDate({"body" : {"game_id" : games[i].game_id, "name" : "points"}}, lastdate,lastmonth,lastweek,function (err, leaderboard) {
                            log.info('Got leaderboard for the game:', lastdate, lastmonth, lastweek, games[i].game_id, leaderboard);

                            //let daily = leaderboard.find(x => x.category === 'daily');
                            // let monthly = leaderboard.find(x => x.category === 'monthly');
                            let weekly = leaderboard.find(x => x.category === 'weekly');

                            let weekly_limit = games[i].leaderboard.to_reward.weekly;
                            let places = [];
                            for (let j in weekly.scores) {
                                places.push({"place" : Object.keys(weekly.scores).indexOf(j)+1, "player_id" : j, "score" : weekly.scores[j]})
                            }

                            log.warn('Got places: TOP', weekly_limit, places.slice(0, weekly_limit))
                            let rewarded = places.slice(0, weekly_limit);

                            //Reformat leaderboard daily/weekly/monthly data
                            let weekly_reformatted = [];
                            for (let k in games[i].leaderboard.gifts.weekly ) {
                                if (typeof ( games[i].leaderboard.gifts.weekly[k].place) === 'string') {
                                    reformat_weekly(
                                        games[i].leaderboard.gifts.weekly[k].place.split('-')[0],
                                        games[i].leaderboard.gifts.weekly[k].place.split('-')[1],
                                        games[i].leaderboard.gifts.weekly[k])
                                } else {
                                    weekly_reformatted.push( games[i].leaderboard.gifts.weekly[k])
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
                            for (let x in rewarded){
                                //Pushing to accelera
                                try {
                                    let reward = weekly_reformatted[rewarded[x].place-1].reward;
                                    let context = {
                                        "place" : rewarded[x].place,
                                        "range" : weekly["range"],
                                        "score" : parseInt(rewarded[x].score),
                                        "player_id" : rewarded[x].player_id,
                                        "reward" : reward,
                                        "sum" : reward.split('-')[1],
                                        "title" : weekly_reformatted[rewarded[x].place-1].title,
                                        "name" : weekly_reformatted[rewarded[x].place-1].name,
                                        "details" : weekly_reformatted[rewarded[x].place-1].details
                                    };
                                    log.warn('Processing reward trigger:', JSON.stringify(context));

                                    //Publish trigger
                                    API.publish(rewarded[x].player_id, 'weekly_reward', context, function (){})


                                } catch (e) {
                                    log.error(e)
                                }

                            }

                        })
                    }
                }
            }
        })
    });
});

job.start();