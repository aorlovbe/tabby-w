let _ = require("lodash");
let moment = require("moment");
const timeZone = require("moment-timezone");
let log = require("../services/bunyan").log;
let Leaderboards = require("../api/leaderboard");
let Game = require("../api/games");
const API = require("../middleware/api");

const producer = require("../services/producer");
const settings = require("../settings");
process.env.TZ = "Europe/Moscow";

// Each 1 day of month at 00:00
//let schedule = '00 00 00 1 * *';
let schedule = "00 40 21 * * *";
var CronJob = require("cron").CronJob;

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer and Game API events consumer */
let job = new CronJob(schedule, function () {
  producer.createProducer(settings.instance).then(function () {
    log.info(
      "Accelera Game API producer is created for schedule:",
      settings.instance,
      schedule
    );
    //Starting schedule
    log.info("Monthly rewarder is started (last month):", schedule);

    //let lastdate = moment(timeZone.tz('Europe/Moscow')).subtract(1,'days').format('YYYY-MM-DD');
    //let lastmonth = moment(timeZone.tz('Europe/Moscow')).subtract(1,'months').format('YYYY-MM');
    //let lastweek = moment(timeZone.tz('Europe/Moscow')).subtract(1,'weeks').format('MM-WW');

    let lastdate = "2024-03-01";
    let lastmonth = "2024-12";
    let lastweek = "01-04";

    Game.list({}, function (err, games) {
      if (Object.keys(games).length !== 0) {
        for (let i in games) {
          if (games[i].game_id === "tabby_dev") {
            Leaderboards.getUnmaskedbyDate(
              { body: { game_id: games[i].game_id, name: "points" } },
              lastdate,
              lastmonth,
              lastweek,
              function (err, leaderboard) {
                log.info(
                  "Got leaderboard for the game:",
                  lastdate,
                  lastmonth,
                  lastweek,
                  games[i].game_id,
                  leaderboard
                );

                //let daily = leaderboard.find(x => x.category === 'daily');
                // let weekly = leaderboard.find(x => x.category === 'weekly');
                let monthly = leaderboard.find((x) => x.category === "monthly");

                let monthly_limit = games[i].leaderboard.to_reward.monthly;
                let places = [];
                for (let i in monthly.scores) {
                  places.push({
                    place: Object.keys(monthly.scores).indexOf(i) + 1,
                    player_id: i,
                    score: monthly.scores[i],
                  });
                }

                log.warn(
                  "Got places: TOP",
                  monthly_limit,
                  places.slice(0, monthly_limit)
                );
                let rewarded = places.slice(0, monthly_limit);

                //Reformat leaderboard daily/weekly/monthly data
                let monthly_reformatted = [];
                for (let k in games[i].leaderboard.gifts.monthly) {
                  if (
                    typeof games[i].leaderboard.gifts.monthly[k].place ===
                    "string"
                  ) {
                    reformat_monthly(
                      games[i].leaderboard.gifts.monthly[k].place.split("-")[0],
                      games[i].leaderboard.gifts.monthly[k].place.split("-")[1],
                      games[i].leaderboard.gifts.monthly[k]
                    );
                  } else {
                    monthly_reformatted.push(
                      games[i].leaderboard.gifts.monthly[k]
                    );
                  }
                }

                function reformat_monthly(from, to, data) {
                  for (let z = parseInt(from); z <= parseInt(to); z++) {
                    let newdata = _.cloneDeep(data);
                    newdata.place = z;
                    monthly_reformatted.push(newdata);
                  }
                }

                //Processing events to monthly scenario
                for (let x in rewarded) {
                  //Pushing to accelera
                  try {
                    let reward = monthly_reformatted[rewarded[x].place - 1].id;
                    let personalized = games[i].rewards.find(
                      (item) => item.id === reward
                    );
                    let context = {
                      place: rewarded[x].place,
                      range: monthly["range"],
                      score: parseInt(rewarded[x].score),
                      player_id: rewarded[x].player_id,
                      id: personalized.id,
                      reward: personalized,
                      title: monthly_reformatted[rewarded[x].place - 1].title,
                      name: monthly_reformatted[rewarded[x].place - 1].name,
                      details:
                        monthly_reformatted[rewarded[x].place - 1].details,
                    };
                    log.warn(
                      "Processing reward trigger:",
                      JSON.stringify(context)
                    );

                    //Publish trigger
                    API.publish(
                      rewarded[x].player_id,
                      "monthly_reward_city",
                      context,
                      function () {}
                    );
                  } catch (e) {
                    log.error(e);
                  }
                }
              }
            );
          }
        }
      }
    });
  });
});

job.start();
