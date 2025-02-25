const polka = require("polka");
const router = polka();
const passport = require("../middleware/passport-auth");
const log = require("../services/bunyan").log;
const SMS = require("../middleware/sms");
const API = require("../middleware/api");
const Token = require("../middleware/tokens");
const Achievement = require("../api/achievements");
const Rewards = require("../api/rewards");
const Counter = require("../api/counters");
const Dialog = require("../api/dialogs");
const Task = require("../api/tasks");
const Increment = require("../api/increments");
const Items = require("../api/items");
const Profile = require("../api/profiles");
const User = require("../api/users");
const Leaderboard = require("../api/leaderboard");
const _ = require("lodash");
const async = require("async");
const utils = require("../services/utils");
//External packs method
const Pack = require("../api/packs");

const send = require("@polka/send-type");
const Game = require("../api/games");
const sha = require("../services/sha");
const redis = require("../services/redis").redisclient_rewarder;
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const bulk = require("../services/bulk");
const Nakama = require("../middleware/nakama");
const crate = require("../services/crateio");

const rateLimit = require("express-rate-limit");
const accelera = require("../services/producer");
const Achievements = require("../api/achievements");
const timeZone = require("moment-timezone");
const nanoid = require("../services/nanoid");
const birthdayLimiter = rateLimit({
  windowMs: 5 * 60 * 1000, // 5 minutes
  keyGenerator: (request, response) => request.ip,
  max: 100, // Limit each IP to 5 requests per `window` (here, per 5 minutes)
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
  message: "Too many requests, please try again later.",
  handler: (request, response, next, options) =>
    send(response, 429, {
      status: "Too many requests, please try again later.",
    }),
});

//Services activation from games (with token)
router.post(
  "/map",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  API.getAdditionalRewardsFromCrate,
  API.getMapSettingsbyProfile,
  API.isBlockedClient,
  API.isBlocked,
  API.isBlockedIP,
  API.reloadTabbyRewardsFromCrate,
  (req, res, next) => {
    let map = {
      status: "ok",
      position:
        req.body.counters.position !== undefined
          ? parseInt(req.body.counters.position)
          : 0,
      balance:
        req.body.counters.balance !== undefined
          ? parseInt(req.body.counters.balance)
          : 0,
      onboarding: true,
      invite: "https://podarki.beeline.ru/?invite=" + req.body.profile_id,
      settings: [],
    };

    //Creating level structure
    // ONBOARDING
    // LEVELS

    async.waterfall([processOnboarding, processLevels], function (err, result) {
      if (err) {
        log.error("Error while collecting a map:", err);
      } else {
        log.info("Map is collected");
        send(res, 200, map);
      }
    });

    function processOnboarding(done) {
      Profile.get(req.body.profile_id, function (err, profile) {
        if (err) {
          done();
        } else {
          map.onboarding = profile.onboarding === "true";
          done();
        }
      });
    }

    function processLevels(done) {
      map.current_time = Math.floor(new Date());

      //Check freezed portals
      if (req.body.game.freezed !== undefined) {
        for (let i in req.body.game.freezed) {
          req.body.levels[req.body.game.freezed[i] - 1].COUNTERVALUE =
            "freezed";
        }
        map.settings = req.body.levels;
      } else {
        map.settings = req.body.levels;
      }
      done();
    }
  }
);

router.post(
  "/balance",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  API.isBlocked,
  Pack.asyncGameList,
  (req, res, next) => {
    let balance =
      req.body.counters.balance === undefined
        ? 0
        : parseInt(req.body.counters.balance);
    let additional_balance =
      req.body.counters.additional_balance === undefined
        ? 0
        : parseInt(req.body.counters.additional_balance);

    //Adding GORKY PARK limit as additional
    let limit =
      req.body.counters.daily_limit === undefined
        ? 50
        : parseInt(req.body.counters.daily_limit);
    let communications =
      req.body.counters.communications === undefined
        ? true
        : req.body.counters.communications === "yes";
    let multipl =
      req.body.counters["booster_speed"] === undefined
        ? 1
        : parseFloat(req.body.counters["booster_speed"]);
    //let multipl = 1;

    //Nearest top gifts position
    let position =
      req.body.counters.position !== undefined
        ? parseInt(req.body.counters.position)
        : 0;
    let nearest = [0, 0, "", ""];
    Profile.getNearestTop(
      position,
      function (_top, _super, _closestTop, _closestSuper) {
        nearest[0] = _top;
        nearest[1] = _super;
        nearest[2] = _closestTop;
        nearest[3] = _closestSuper;
      }
    );

    //Time manipulation for next ticks
    async.waterfall([createIntervals], function (err, intervals) {
      if (err) {
      } else {
        let next_stored_tick =
          req.body.counters.next_tick === undefined
            ? intervals.next_tick
            : parseInt(req.body.counters.next_tick); //when last free try was given
        log.info("Intervals:", JSON.stringify(intervals));
        if (balance < limit) {
          if (next_stored_tick < intervals.time_now) {
            //Give +n & save new next_tick
            //Find next_stored_tick position
            for (let x in intervals.dayarray) {
              if (intervals.dayarray[x] >= next_stored_tick) {
                let toGive = Math.min(
                  intervals.next_tick_pos - x,
                  limit - balance
                );
                log.info("+n is:", toGive, req.body.profile_id);
                //Update next tick in counters
                //Check if next_stored_tick is in previous days
                if (next_stored_tick < intervals.startOfDay) {
                  let t = Math.ceil(
                    (intervals.startOfDay - next_stored_tick) / intervals.period
                  );
                  let j = Math.min(t + toGive + balance, limit);
                  log.info(
                    "Previous tick date is less then today, giving remain",
                    t,
                    t + toGive + balance,
                    limit,
                    req.body.profile_id
                  );
                  toGive = j;
                }

                let attempt = {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "balance",
                  value: balance + toGive <= limit ? toGive : limit - balance, //fix for previous day
                };
                Counter.modify({ body: attempt }, function (err, changes) {
                  balance = changes.balance;
                  let nt = {
                    profile_id: req.body.profile_id,
                    game_id: req.body.game.game_id,
                    name: "next_tick",
                    value: intervals.next_tick,
                  };
                  Counter.create({ body: nt }, function (err, changes) {});
                });
                break;
              }
            }
          } else {
            log.info("Not ready to give +n", req.body.profile_id);
            if (req.body.counters.next_tick === undefined) {
              let nt = {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                name: "next_tick",
                value: intervals.next_tick,
              };
              Counter.create({ body: nt }, function (err, changes) {});
            }
          }
        } else {
          //Исправление баланса если он больше лимита - так получилост по ошибке выше
          let fix = {
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            name: "balance",
            value: limit,
          };
          Counter.create({ body: fix }, function (err, changes) {
            balance = changes.balance;
          });
        }

        // push, yung, bazya, tapa, ping
        Profile.get(req.body.profile_id, function (err, profile) {
          let portal_active = req.body.counters.portal_opts !== undefined;

          //Check for expired limits
          let limit_booster =
            req.body.counters.limit_booster !== undefined
              ? JSON.parse(req.body.counters.limit_booster)
              : "";
          let rating_booster =
            req.body.counters.rating_booster !== undefined
              ? JSON.parse(req.body.counters.rating_booster)
              : "";
          let speed_booster =
            req.body.counters.speed_booster !== undefined
              ? JSON.parse(req.body.counters.speed_booster)
              : "";

          checkLimitsExpiration(limit_booster, function () {
            checkRatingsExpiration(rating_booster, function () {
              checkSpeedExpiration(speed_booster, function () {
                //Check for active boosters
                req.body.counters.active_casts =
                  req.body.counters.limit_booster !== undefined
                    ? JSON.parse(req.body.counters.limit_booster).value
                    : "";
                req.body.counters.active_rating =
                  req.body.counters.rating_booster !== undefined
                    ? JSON.parse(req.body.counters.rating_booster).value
                    : "";
                req.body.counters.active_speed =
                  req.body.counters.speed_booster !== undefined
                    ? JSON.parse(req.body.counters.speed_booster).value
                    : "";

                //New time elapsed gifts
                let active_timer_rewards = 0;
                let map = {
                  status: "ok",
                  is_debtor: req.body.disabledDueToDebts,
                  active_timer_rewards: active_timer_rewards,
                  balance: balance + additional_balance, //баланс от выполнения заданий
                  communications: communications,
                  limit: limit,
                  ending: getEnding(balance + additional_balance),
                  next_tries: 1,
                  character:
                    profile.character === undefined
                      ? "push"
                      : profile.character,
                  onboarding: profile.onboarding === "true",
                  timer_active: balance !== limit,
                  time_now: intervals.time_now,
                  time_end: intervals.next_tick,
                  counters: req.body.counters,
                  portal_active: portal_active,
                  portal_opts:
                    portal_active === true
                      ? JSON.parse(req.body.counters.portal_opts)
                      : [],
                  nearest_prizes: {
                    top_text: nearest[0] + getEndingNearest(nearest[0]),
                    super_text: nearest[1] + getEndingNearest(nearest[1]),
                    top_id: nearest[2],
                    super_id: nearest[3],
                  },
                };

                send(res, 200, map);
              });
            });
          });
        });

        function getEndingNearest(num) {
          let last = num.toString().slice(-1);
          let ord = "";

          switch (last) {
            case "1":
              if (num.toString() === "111") {
                ord = " клеток";
              } else {
                ord = " клетка";
              }
              break;
            case "2":
              ord = " клетки";
              break;
            case "3":
              ord = " клетки";
              break;
            case "4":
              ord = " клетки";
              break;
            default:
              ord = " клеток";
              break;
          }

          return ord;
        }

        function checkLimitsExpiration(boost, callback) {
          let current = Math.floor(new Date());

          if (boost !== "" && boost.expired_at < current) {
            //Expired so return limits to 50
            Counter.create(
              {
                body: {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "daily_limit",
                  value: 50,
                },
              },
              function (err, done) {
                delete req.body.counters["limit_booster"];

                //Removing booster from counters
                Counter.remove(
                  {
                    body: {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "limit_booster",
                    },
                  },
                  function (err, done) {
                    callback();
                  }
                );
              }
            );
          } else {
            callback();
          }
        }

        function checkRatingsExpiration(boost, callback) {
          let current = Math.floor(new Date());

          if (boost !== "" && boost.expired_at < current) {
            delete req.body.counters["rating_booster"];

            //Removing booster from counters
            Counter.remove(
              {
                body: {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "rating_booster",
                },
              },
              function (err, done) {
                callback();
              }
            );
          } else {
            callback();
          }
        }

        function checkSpeedExpiration(boost, callback) {
          let current = Math.floor(new Date());

          if (boost !== "" && boost.expired_at < current) {
            delete req.body.counters["speed_booster"];

            //Removing booster from counters
            Counter.remove(
              {
                body: {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "speed_booster",
                },
              },
              function (err, done) {
                callback();
              }
            );
          } else {
            callback();
          }
        }

        // Balance call
        // {
        // 	"status": "ok",
        // 	"balance": 5,
        // 	"ending": " бросков",
        // 	"next_tries": 10,
        // 	"character": "push",
        // 	"onboarding": true,
        // 	"time_now": 1648128642000,
        // 	"time_end": 1648128642000
        // }

        function getEnding(num) {
          let last = num.toString().slice(-1);
          let ord = "";

          switch (last) {
            case "1":
              if (num.toString() === "111") {
                ord = " бросков";
              } else {
                ord = " бросок";
              }
              break;
            case "2":
              ord = " броска";
              break;
            case "3":
              ord = " броска";
              break;
            case "4":
              ord = " броска";
              break;
            default:
              ord = " бросков";
              break;
          }

          return ord;
        }
      }
    });

    function createIntervals(done) {
      const interval_24 = 1000 * 60 * 60 * 24; // 24 hours in milliseconds
      let startOfDay_ = new Date();
      startOfDay_.setHours(0, 0, 0, 0);
      let startOfDay = Math.floor(startOfDay_);
      let endOfDay = startOfDay + interval_24 - 1;

      let time_now = Math.floor(new Date());

      let time_end = Math.round(
        time_now + (24 * 60 * 60 * 1000) / (limit * multipl)
      );
      let period = time_end - time_now; //ms period

      let dayarray = [];
      let i = startOfDay;
      while (i <= endOfDay) {
        dayarray.push(i);
        i = i + period;
      }
      dayarray.push(endOfDay);

      log.info("Daily array is:", dayarray, req.body.profile_id);
      let next_tick;
      let next_tick_pos;
      for (let x in dayarray) {
        if (dayarray[x] >= time_now) {
          next_tick = dayarray[x];
          next_tick_pos = x;
          break;
        }
      }

      done(null, {
        next_tick: next_tick,
        next_tick_pos: next_tick_pos,
        startOfDay: startOfDay,
        endOfDay: endOfDay,
        dayarray: dayarray,
        time_now: time_now,
        time_end: time_end,
        period: period,
      });
    }
  }
);

router.post(
  "/lookup",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.getMapSettingsbyProfile,
  API.AcceleraCoupons,
  (req, res, next) => {
    if (req.body.game.freezed !== undefined) {
      if (req.body.game.freezed.includes(req.body.id) === true) {
        send(res, 200, {
          status: "ok",
          gifts: [
            {
              id: "freezed",
              icon: "freezed",
              title: "Портал заморожен",
              short_description: "Сегодня вам повезло! Этот портал не работает",
            },
          ],
        });
      } else {
        proceed();
      }
    } else {
      proceed();
    }

    function proceed() {
      try {
        let next_on_map = _.find(req.body.levels, function (l) {
          return l.COUNTERKEY === req.body.id;
        });

        let gift = _.find(req.body.game.rewards, function (g) {
          return g.id === next_on_map.COUNTERVALUE;
        });

        //Check remains
        if (
          (gift.activation_type === "unique" &&
            gift.id.includes("top-") === true) ||
          gift.id === "top-100" ||
          gift.id === "top-101"
        ) {
          Profile.getTabbyTopRemain(gift, function (err, remain) {
            send(res, 200, {
              status: "ok",
              gifts: [
                {
                  id: gift.id,
                  icon: gift.id,
                  title: gift.title,
                  short_description:
                    gift.short_description +
                    ".\nОсталось " +
                    formatNumber(remain) +
                    " призов",
                },
              ],
            });
          });
        } else {
          if (
            gift.prize_type === "collection-supergift" ||
            gift.prize_type === "collection"
          ) {
            let collection = req.body.game.collections[gift.category];
            collection.remain =
              collection.accelera_stack !== undefined
                ? req.body.accelera_coupons[collection.accelera_stack].size
                : 0;

            send(res, 200, {
              status: "ok",
              gifts: [
                {
                  id: gift.id,
                  icon: gift.id,
                  title: gift.title,
                  short_description: gift.short_description,
                  collection: collection,
                },
              ],
            });
          } else {
            send(res, 200, {
              status: "ok",
              gifts: [
                {
                  id: gift.id,
                  icon: gift.id,
                  title: gift.title,
                  short_description: gift.short_description,
                },
              ],
            });
          }
        }

        function formatNumber(num) {
          return num.toString().replace(/(\d)(?=(\d{3})+(?!\d))/g, "$1 ");
        }

        //Update analytics
        let event = {
          event: "accelera-api",
          page: "webhooks",
          status: "webhook",
          game_id: req.body.game.game_id,
          details: "lookup",
          context: JSON.stringify({ id: gift.id }),
          profile_id: req.body.profile_id,
          player_id:
            req.body.player_id === undefined
              ? ""
              : req.body.player_id.toString(),
          timestamp: Math.floor(new Date()),
          date: moment(new Date()).format("YYYY-MM-DD"),
          time: moment(new Date()).format("HH:mm"),
          datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
            "YYYY-MM-DD HH:mm:ss"
          ),
        };

        bulk.store(
          req.body.game.game_id,
          JSON.stringify(event),
          function (err) {
            if (err) {
              log.error(
                "Error while storing webhooks messages for Clickhouse bulk:",
                err
              );
            }
          }
        );
      } catch (e) {
        log.error("Trying to lookup but error:", req.body.id);
        send(res, 200, {
          status: "ok",
          gifts: [
            {
              id: "treasure_partner",
              title: "Тут приза нет",
              short_description: "Продолжай свой путь к призам!",
            },
          ],
        });
      }
    }
  }
);

router.post(
  "/step",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  API.getMapSettingsbyProfile,
  Profile.getTabbyPersonalPartners,
  API.isBlockedClient,
  API.isBlocked,
  API.isBlockedIP,
  Profile.defineTabbyRatingBooster,
  Profile.defineIsPortalActive,
  API.AcceleraCoupons,
  (req, res, next) => {
    let position =
      req.body.counters.position === undefined
        ? 0
        : parseInt(req.body.counters.position);
    let balance =
      req.body.counters.balance === undefined
        ? 0
        : parseInt(req.body.counters.balance);

    //New for limited time rewards
    //TODO update
    let active_timer_rewards = 0;

    //Adding additional_balance as additional , after tasks
    let additional_balance =
      req.body.counters.additional_balance === undefined
        ? 0
        : parseInt(req.body.counters.additional_balance);
    let limit =
      req.body.counters.daily_limit === undefined
        ? 50
        : parseInt(req.body.counters.daily_limit);

    let multipl =
      req.body.counters["booster_speed"] === undefined
        ? 1
        : parseFloat(req.body.counters["booster_speed"]);
    //let multipl = 1;
    //let multiply_rating = (req.body.counters["multiply_rating"] === undefined) ? 1 : parseInt(req.body.counters["multiply_rating"]);
    let multiply_rating = req.body.defined_rating_multiplier; //From profiles
    //let multiply_rating = 1;

    let step = Math.floor(Math.random() * 6) + 1; //Кубик 1-6
    //let step = 1; //Кубик 1
    //Getting next position and a gift
    let next_position = position + step;
    let next_tries = 1;
    let timer_active = balance + additional_balance < limit;

    let nearest = [0, 0, "", ""];
    Profile.getNearestTop(
      next_position,
      function (_top, _super, _closestTop, _closestSuper) {
        nearest[0] = _top;
        nearest[1] = _super;
        nearest[2] = _closestTop;
        nearest[3] = _closestSuper;
      }
    );

    let this_on_map = _.find(req.body.levels, function (l) {
      return l.COUNTERKEY === position;
    });
    let type_cell = position === 0 ? "start" : this_on_map.COUNTERVALUE;

    //Replacement with freezed
    if (req.body.game.freezed !== undefined) {
      for (let i in req.body.game.freezed) {
        //log.warn('Freezed replaced')
        req.body.levels[req.body.game.freezed[i] - 1].COUNTERVALUE = "freezed";
      }
      //log.warn('Freezed replacement done')
    }

    //Уникальный ID броска
    let last_step_uuid = nanoid.get();
    let stepID = {
      profile_id: req.body.profile_id,
      game_id: req.body.game.game_id,
      name: "last_step_uuid",
      value: last_step_uuid,
    };
    Counter.create({ body: stepID }, function (err, changes) {});

    function createIntervals(done) {
      const interval_24 = 1000 * 60 * 60 * 24; // 24 hours in milliseconds
      let startOfDay_ = new Date();
      startOfDay_.setHours(0, 0, 0, 0);
      let startOfDay = Math.floor(startOfDay_);
      let endOfDay = startOfDay + interval_24 - 1;

      let time_now = Math.floor(new Date());

      let time_end = Math.round(
        time_now + (24 * 60 * 60 * 1000) / (limit * multipl)
      );
      let period = time_end - time_now; //ms period

      let dayarray = [];
      let i = startOfDay;
      while (i <= endOfDay) {
        dayarray.push(i);
        i = i + period;
      }
      dayarray.push(endOfDay);

      log.info("Daily array is:", dayarray, req.body.profile_id);
      let next_tick;
      let next_tick_pos;
      for (let x in dayarray) {
        if (dayarray[x] >= time_now) {
          next_tick = dayarray[x];
          next_tick_pos = x;
          break;
        }
      }

      done(null, {
        next_tick: next_tick,
        next_tick_pos: next_tick_pos,
        startOfDay: startOfDay,
        endOfDay: endOfDay,
        dayarray: dayarray,
        time_now: time_now,
        time_end: time_end,
        period: period,
      });
    }

    createIntervals(function (err, intervals) {
      if (balance + additional_balance > 0) {
        //Check if finished
        let attempt = {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
          name: additional_balance > 0 ? "additional_balance" : "balance",
          value: -1,
        };

        Counter.modify({ body: attempt }, function (err, changes) {
          //Update because can be additional_balance
          changes.balance =
            changes.additional_balance === undefined
              ? changes.balance
              : balance + parseInt(changes.additional_balance);

          if (parseInt(changes.balance) === 0) {
            //Pushing to accelera
            accelera
              .publishTrigger(req.body.profile_id, "zero-balance", {
                game_id: req.body.game.game_id,
                profile_id: req.body.profile_id,
                player_id: req.body.player_id,
              })
              .then(function () {
                log.info(
                  "Trigger was published:",
                  "zero-balance",
                  req.body.profile_id
                );
              })
              .catch((e) => {
                log.error("Failed to publish trigger:", e);
              });
          }

          if (!err || parseInt(changes.balance) >= 0) {
            if (next_position >= 600) {
              //Returning portal
              let gift = _.find(req.body.game.rewards, function (g) {
                return g.id === "portal";
              });

              //Setting position to 0 in redis and 600 in feedback
              let move = {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                name: "position",
                value: 0,
              };

              accelera
                .publishTrigger(req.body.profile_id, "new-round", {
                  game_id: req.body.game.game_id,
                  profile_id: req.body.profile_id,
                  player_id: req.body.player_id,
                })
                .then(function () {
                  log.info(
                    "Trigger was published:",
                    "new-round",
                    req.body.profile_id
                  );
                })
                .catch((e) => {
                  log.error("Failed to publish trigger:", e);
                });

              Counter.create({ body: move }, function (err, done) {
                gift.icon = gift.id;
                send(res, 200, {
                  status: "ok",
                  step: 600 - position,
                  position: 600,
                  balance: parseInt(changes.balance),
                  counters: req.body.counters,
                  ending:
                    changes.balance !== "0"
                      ? getEnding(changes.balance)
                      : getEnding(1),
                  gifts: [gift],
                  is_finished: true,
                  same_elements: false,
                  same_promocode: false,
                  promocode: "",
                  limit: limit,
                  timer_active: timer_active,
                  next_tries: next_tries,
                  time_now: intervals.time_now,
                  time_end: intervals.next_tick,
                  active_timer_rewards: active_timer_rewards,
                  nearest_prizes: {
                    top_text: nearest[0] + getEndingNearest(nearest[0]),
                    super_text: nearest[1] + getEndingNearest(nearest[1]),
                    top_id: nearest[2],
                    super_id: nearest[3],
                  },
                });

                //Also reload req.body.counters.steps_stay_here counter to 8 because finished
                let steps_stay_here = {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "steps_stay_here",
                  value: 10,
                };

                Counter.create(
                  { body: steps_stay_here },
                  function (err, done) {}
                );
              });

              //Update analytics
              let event = {
                event: "accelera-api",
                jwt: req.body.token,
                page: "map",
                status: "step",
                game_id: req.body.game.game_id,
                additional: last_step_uuid,
                details: step.toString(),
                gifts: [position.toString(), type_cell, "0", "start"],
                profile_id: req.body.profile_id,
                player_id:
                  req.body.player_id === undefined
                    ? ""
                    : req.body.player_id.toString(),
                timestamp: Math.floor(new Date()),
                date: moment(new Date()).format("YYYY-MM-DD"),
                time: moment(new Date()).format("HH:mm"),
                datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
                  "YYYY-MM-DD HH:mm:ss"
                ),
              };

              bulk.store(
                req.body.game.game_id,
                JSON.stringify(event),
                function (err) {
                  if (err) {
                    log.error(
                      "Error while storing webhooks messages for Clickhouse bulk:",
                      err
                    );
                  }
                }
              );
            } else {
              let next_on_map = _.find(req.body.levels, function (l) {
                return l.COUNTERKEY === next_position;
              });

              async.waterfall([processGift], function (err, result) {
                if (err) {
                  log.error("Error while stepping a gift:", err);
                  send(res, 500, {
                    status: "not_enough_balance",
                    step: 0,
                    position: next_position,
                    balance: parseInt(changes.balance),
                    counters: req.body.counters,
                    ending:
                      changes.balance !== "0"
                        ? getEnding(changes.balance)
                        : getEnding(1),
                    gifts: [],
                    is_finished: false,
                    same_elements: false,
                    same_promocode: false,
                    promocode: "",
                    limit: limit,
                    timer_active: timer_active,
                    next_tries: next_tries,
                    time_now: intervals.time_now,
                    time_end: intervals.next_tick,
                    active_timer_rewards: active_timer_rewards,
                    nearest_prizes: {
                      top_text: nearest[0] + getEndingNearest(nearest[0]),
                      super_text: nearest[1] + getEndingNearest(nearest[1]),
                      top_id: nearest[2],
                      super_id: nearest[3],
                    },
                  });
                } else {
                  if (result.id === "sd-5" || result.id === "s-2") {
                    //Store move
                    let move = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "position",
                      value: step,
                    };

                    Counter.modify({ body: move }, function (err, done) {});

                    //Pushing to accelera
                    accelera
                      .publishTrigger(req.body.profile_id, "Tabby-attempt", {
                        game_id: req.body.game.game_id,
                        profile_id: req.body.profile_id,
                        player_id: req.body.player_id,
                        reward: result,
                        sms_type: result.sms_type,
                        last_step_uuid: last_step_uuid,
                        ip: req.body.request_ip,
                      })
                      .then(function () {
                        log.info(
                          "Trigger was published:",
                          "Tabby-attempt",
                          req.body.profile_id,
                          result
                        );
                      })
                      .catch((e) => {
                        log.error("Failed to publish trigger:", e);
                      });

                    //Новые соты
                    let sot_val = parseInt(result.id.split("-")[1]);
                    let current_month = moment(new Date()).format("YYYY-MM");
                    let sot =
                      req.body.counters.beeline_sot === undefined
                        ? 0
                        : parseInt(req.body.counters.beeline_sot);
                    let mon =
                      req.body.counters.beeline_month === undefined
                        ? current_month
                        : req.body.counters.beeline_month;

                    if (mon !== current_month) {
                      //New month
                      //Pushing to accelera
                      accelera
                        .publishTrigger(req.body.profile_id, "Tabby-sot", {
                          game_id: req.body.game.game_id,
                          profile_id: req.body.profile_id,
                          player_id: req.body.player_id,
                          reward: result,
                          value: sot_val,
                          service_code: result.service_code,
                          new_month: "true",
                        })
                        .then(function () {
                          log.info(
                            "Trigger was published:",
                            "Tabby-sot",
                            req.body.profile_id,
                            result
                          );
                        })
                        .catch((e) => {
                          log.error("Failed to publish trigger:", e);
                        });

                      //Updating new month
                      Counter.create(
                        {
                          body: {
                            profile_id: req.body.profile_id,
                            game_id: req.body.game.game_id,
                            name: "beeline_month",
                            value: current_month,
                          },
                        },
                        function (err, done) {
                          //Reply
                          send(res, 200, {
                            status: "ok",
                            step: step,
                            position: next_position,
                            counters: req.body.counters,
                            balance: parseInt(changes.balance),
                            ending:
                              changes.balance !== "0"
                                ? getEnding(changes.balance)
                                : getEnding(1),
                            gifts: [result],
                            is_finished: false,
                            same_elements: false,
                            same_promocode: false,
                            promocode: "",
                            limit: limit,
                            timer_active: timer_active,
                            next_tries: next_tries,
                            time_now: intervals.time_now,
                            time_end: intervals.next_tick,
                            active_timer_rewards: active_timer_rewards,
                            nearest_prizes: {
                              top_text:
                                nearest[0] + getEndingNearest(nearest[0]),
                              super_text:
                                nearest[1] + getEndingNearest(nearest[1]),
                              top_id: nearest[2],
                              super_id: nearest[3],
                            },
                          });
                        }
                      );
                    } else {
                      //Current month
                      if (sot + sot_val <= 100) {
                        //Pushing to accelera
                        accelera
                          .publishTrigger(req.body.profile_id, "Tabby-sot", {
                            game_id: req.body.game.game_id,
                            profile_id: req.body.profile_id,
                            player_id: req.body.player_id,
                            reward: result,
                            value: sot_val,
                            service_code: result.service_code,
                            new_month: "false",
                          })
                          .then(function () {
                            log.info(
                              "Trigger was published:",
                              "Tabby-sot",
                              req.body.profile_id,
                              result
                            );
                          })
                          .catch((e) => {
                            log.error("Failed to publish trigger:", e);
                          });

                        send(res, 200, {
                          status: "ok",
                          step: step,
                          position: next_position,
                          counters: req.body.counters,
                          balance: parseInt(changes.balance),
                          ending:
                            changes.balance !== "0"
                              ? getEnding(changes.balance)
                              : getEnding(1),
                          gifts: [result],
                          is_finished: false,
                          same_elements: false,
                          same_promocode: false,
                          promocode: "",
                          limit: limit,
                          timer_active: timer_active,
                          next_tries: next_tries,
                          time_now: intervals.time_now,
                          time_end: intervals.next_tick,
                          active_timer_rewards: active_timer_rewards,
                          nearest_prizes: {
                            top_text: nearest[0] + getEndingNearest(nearest[0]),
                            super_text:
                              nearest[1] + getEndingNearest(nearest[1]),
                            top_id: nearest[2],
                            super_id: nearest[3],
                          },
                        });
                      } else {
                        //Its a cap, showing different words
                        result.short_description = result.short_description1;

                        send(res, 200, {
                          status: "ok",
                          step: step,
                          position: next_position,
                          counters: req.body.counters,
                          balance: parseInt(changes.balance),
                          ending:
                            changes.balance !== "0"
                              ? getEnding(changes.balance)
                              : getEnding(1),
                          gifts: [result],
                          is_finished: false,
                          same_elements: false,
                          same_promocode: false,
                          promocode: "",
                          limit: limit,
                          timer_active: timer_active,
                          next_tries: next_tries,
                          time_now: intervals.time_now,
                          time_end: intervals.next_tick,
                          active_timer_rewards: active_timer_rewards,
                          nearest_prizes: {
                            top_text: nearest[0] + getEndingNearest(nearest[0]),
                            super_text:
                              nearest[1] + getEndingNearest(nearest[1]),
                            top_id: nearest[2],
                            super_id: nearest[3],
                          },
                        });
                      }
                    }
                  } else if (result.activation_type === "discount") {
                    log.info("Its a discount reward");
                    //Check if I already have this gift
                    Rewards.findbyprofile(req, function (err, issued) {
                      let issued_partners = Object.keys(issued);
                      if (issued_partners.includes(result.id) === false) {
                        //New
                        //Store move
                        result.created_timestamp = Math.floor(new Date());
                        let move = {
                          profile_id: req.body.profile_id,
                          game_id: req.body.game.game_id,
                          name: "position",
                          value: step,
                        };

                        Counter.modify({ body: move }, function (err, done) {
                          send(res, 200, {
                            status: "ok",
                            step: step,
                            position: next_position,
                            counters: req.body.counters,
                            balance: parseInt(changes.balance),
                            ending:
                              changes.balance !== "0"
                                ? getEnding(changes.balance)
                                : getEnding(1),
                            gifts: [result],
                            is_finished: false,
                            same_elements: false,
                            same_promocode: false,
                            promocode: "",
                            limit: limit,
                            timer_active: timer_active,
                            next_tries: next_tries,
                            time_now: intervals.time_now,
                            time_end: intervals.next_tick,
                            active_timer_rewards: active_timer_rewards,
                            nearest_prizes: {
                              top_text:
                                nearest[0] + getEndingNearest(nearest[0]),
                              super_text:
                                nearest[1] + getEndingNearest(nearest[1]),
                              top_id: nearest[2],
                              super_id: nearest[3],
                            },
                          });

                          result.sms_type = "0";
                          //Pushing to accelera
                          accelera
                            .publishTrigger(
                              req.body.profile_id,
                              "Tabby-attempt",
                              {
                                game_id: req.body.game.game_id,
                                profile_id: req.body.profile_id,
                                player_id: req.body.player_id,
                                reward: result,
                                sms_type: result.sms_type,
                                last_step_uuid: last_step_uuid,
                                ip: req.body.request_ip,
                              }
                            )
                            .then(function () {
                              log.info(
                                "Trigger was published:",
                                "Tabby-attempt",
                                req.body.profile_id,
                                result
                              );
                            })
                            .catch((e) => {
                              log.error("Failed to publish trigger:", e);
                            });
                        });
                      } else {
                        //Store move
                        let existing_reward = issued[result.id];
                        let created_timestamp = parseInt(
                          existing_reward.created_timestamp
                        );
                        let seconds_to_archive = parseInt(
                          existing_reward.seconds_to_archive
                        );
                        let now = Math.floor(new Date());
                        let end = created_timestamp + seconds_to_archive * 1000;
                        existing_reward.time_end =
                          now > end ? 0 : Math.round((end - now) / 1000);
                        let gamerewards = _.find(req.body.game.rewards, {
                          id: result.id,
                        });
                        existing_reward.is_bought =
                          existing_reward.is_bought === undefined
                            ? false
                            : existing_reward.is_bought !== "false";
                        existing_reward.images = gamerewards.images;

                        let move = {
                          profile_id: req.body.profile_id,
                          game_id: req.body.game.game_id,
                          name: "position",
                          value: step,
                        };

                        Counter.modify({ body: move }, function (err, done) {
                          send(res, 200, {
                            status: "ok",
                            step: step,
                            position: next_position,
                            counters: req.body.counters,
                            balance: parseInt(changes.balance),
                            ending:
                              changes.balance !== "0"
                                ? getEnding(changes.balance)
                                : getEnding(1),
                            gifts: [existing_reward],
                            is_finished: false,
                            same_elements: false,
                            same_promocode: true,
                            promocode: "",
                            limit: limit,
                            timer_active: timer_active,
                            next_tries: next_tries,
                            time_now: intervals.time_now,
                            time_end: intervals.next_tick,
                            active_timer_rewards: active_timer_rewards,
                            nearest_prizes: {
                              top_text:
                                nearest[0] + getEndingNearest(nearest[0]),
                              super_text:
                                nearest[1] + getEndingNearest(nearest[1]),
                              top_id: nearest[2],
                              super_id: nearest[3],
                            },
                          });

                          result.sms_type = "0";
                          //Pushing to accelera
                          /*accelera.publishTrigger(req.body.profile_id, "Tabby-attempt", {
                                                    "game_id" : req.body.game.game_id,
                                                    "profile_id" : req.body.profile_id,
                                                    "player_id" : req.body.player_id,
                                                    "reward" : result,
                                                    "sms_type" : result.sms_type,
                                                    "last_step_uuid" : last_step_uuid
                                                }).then(function (){
                                                    log.info('Trigger was published:', "Tabby-attempt", req.body.profile_id, result);
                                                }).catch(e => {
                                                    log.error('Failed to publish trigger:', e);
                                                });*/
                        });
                      }
                    });
                  } else if (result.id === "freezed") {
                    //Store move
                    let move = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "position",
                      value: step,
                    };

                    Counter.modify({ body: move }, function (err, done) {
                      send(res, 200, {
                        status: "ok",
                        step: step,
                        position: next_position,
                        counters: req.body.counters,
                        balance: parseInt(changes.balance),
                        ending:
                          changes.balance !== "0"
                            ? getEnding(changes.balance)
                            : getEnding(1),
                        gifts: [result],
                        is_finished: false,
                        same_elements: false,
                        same_promocode: false,
                        promocode: "",
                        limit: limit,
                        timer_active: timer_active,
                        next_tries: next_tries,
                        time_now: intervals.time_now,
                        time_end: intervals.next_tick,
                        active_timer_rewards: active_timer_rewards,
                        nearest_prizes: {
                          top_text: nearest[0] + getEndingNearest(nearest[0]),
                          super_text: nearest[1] + getEndingNearest(nearest[1]),
                          top_id: nearest[2],
                          super_id: nearest[3],
                        },
                      });
                    });
                  } else if (result.id === "portal") {
                    let variants = [
                      utils.getRandomIntInclusive(40, 49),
                      utils.getRandomIntInclusive(106, 115),
                      utils.getRandomIntInclusive(143, 149),
                    ];

                    let direction = [
                      variants[0] > next_position ? "Вперед на " : "Назад на ",
                      variants[1] > next_position ? "Вперед на " : "Назад на ",
                      variants[2] > next_position ? "Вперед на " : "Назад на ",
                    ];
                    let steps_stay_here =
                      req.body.counters.steps_stay_here === undefined
                        ? 10
                        : parseInt(req.body.counters.steps_stay_here);
                    let stay_data = _.find(req.body.game.shop.portals, {
                      stays_left: steps_stay_here,
                    });

                    let portal_opts = [
                      { id: "p-0", text: "В начало карты", tile_id: 0 },
                      {
                        id: "p-0",
                        text:
                          direction[0] +
                          Math.abs(next_position - variants[0]) +
                          utils.getStepEnding(
                            Math.abs(next_position - variants[0])
                          ),
                        confirm_text: "Да",
                        tile_id: variants[0],
                      },
                      {
                        id: "p-1",
                        text:
                          direction[1] +
                          Math.abs(next_position - variants[1]) +
                          utils.getStepEnding(
                            Math.abs(next_position - variants[1])
                          ) +
                          " - 2 Р",
                        confirm_text: "Да - 2 Р",
                        tile_id: variants[1],
                      },
                      {
                        id: "p-2",
                        text:
                          direction[2] +
                          Math.abs(next_position - variants[2]) +
                          utils.getStepEnding(
                            Math.abs(next_position - variants[2])
                          ) +
                          " - 6 Р",
                        confirm_text: "Да - 6 Р",
                        tile_id: variants[2],
                      },
                      {
                        discount: stay_data.discount,
                        id: stay_data.id,
                        text: "Остаться - " + stay_data.price,
                        is_available: steps_stay_here !== 0,
                        tile_id: next_position,
                        counter: steps_stay_here,
                      },
                    ];

                    //TODO: поменять счетчик counter в Остаться (можно 8 раз круг) - в самое начало, назад 2 пункт или весь лабиринт - это круг

                    let move_opts = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "portal_opts",
                      value: JSON.stringify(portal_opts),
                    };

                    result["portal_opts"] = portal_opts;

                    Counter.create({ body: move_opts }, function (err, done) {
                      //Store move
                      let move = {
                        profile_id: req.body.profile_id,
                        game_id: req.body.game.game_id,
                        name: "position",
                        value: step,
                      };

                      Counter.modify({ body: move }, function (err, done) {
                        send(res, 200, {
                          status: "ok",
                          step: step,
                          position: next_position,
                          counters: req.body.counters,
                          balance: parseInt(changes.balance),
                          ending:
                            changes.balance !== "0"
                              ? getEnding(changes.balance)
                              : getEnding(1),
                          gifts: [result],
                          is_finished: false,
                          same_elements: false,
                          same_promocode: false,
                          promocode: "",
                          limit: limit,
                          timer_active: timer_active,
                          next_tries: next_tries,
                          time_now: intervals.time_now,
                          time_end: intervals.next_tick,
                          active_timer_rewards: active_timer_rewards,
                          nearest_prizes: {
                            top_text: nearest[0] + getEndingNearest(nearest[0]),
                            super_text:
                              nearest[1] + getEndingNearest(nearest[1]),
                            top_id: nearest[2],
                            super_id: nearest[3],
                          },
                        });
                      });
                    });
                  } else {
                    let move = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "position",
                      value: step,
                    };

                    Counter.modify({ body: move }, function (err, done) {
                      let move_opts = {
                        profile_id: req.body.profile_id,
                        game_id: req.body.game.game_id,
                        name: "portal_opts",
                      };

                      //Removing portal options
                      Counter.remove({ body: move_opts }, function (err, done) {
                        //Pushing to accelera
                        //Check if it is a top gift, no need to take promocode
                        if (result.id.includes("top-") === false) {
                          //Check if I already have this gift
                          Rewards.findbyprofile(req, function (err, issued) {
                            let issued_partners = Object.keys(issued);
                            //Повторно можно получать очки и коллекции
                            let can_be_issued_again =
                              result.id.includes("r-") === true ||
                              result.id.includes("c-") === true;

                            if (
                              issued_partners.includes(result.id) === false ||
                              can_be_issued_again === true
                            ) {
                              //No such gift or can be issued again
                              Profile.getTabbyCoupon(
                                result,
                                function (err, personalized_reward, remain) {
                                  if (!err) {
                                    personalized_reward.promocode =
                                      personalized_reward.coupon;
                                    personalized_reward.remain = remain;
                                    let updatedlink = decodeHTMLEntities(
                                      personalized_reward.link
                                    );

                                    if (
                                      personalized_reward.prize_type ===
                                        "collection-supergift" ||
                                      personalized_reward.prize_type ===
                                        "collection"
                                    ) {
                                      let collection =
                                        req.body.game.collections[
                                          personalized_reward.category
                                        ];
                                      collection.remain =
                                        collection.accelera_stack !== undefined
                                          ? req.body.accelera_coupons[
                                              collection.accelera_stack
                                            ].size
                                          : 0;
                                      personalized_reward.collection =
                                        collection;
                                    }

                                    utils.makeShort(
                                      updatedlink,
                                      function (link) {
                                        //https://dvizh.beeline.ru/?x=
                                        personalized_reward.link = updatedlink;
                                        personalized_reward.shortlink = link;

                                        send(res, 200, {
                                          status: "ok",
                                          step: step,
                                          position: next_position,
                                          counters: req.body.counters,
                                          balance: parseInt(changes.balance),
                                          ending:
                                            changes.balance !== "0"
                                              ? getEnding(changes.balance)
                                              : getEnding(1),
                                          gifts: [personalized_reward],
                                          is_finished: false,
                                          same_elements: false,
                                          same_promocode: false,
                                          remain: remain,
                                          promocode: "",
                                          limit: limit,
                                          timer_active: timer_active,
                                          next_tries: next_tries,
                                          time_now: intervals.time_now,
                                          time_end: intervals.next_tick,
                                          active_timer_rewards:
                                            active_timer_rewards,
                                          nearest_prizes: {
                                            top_text:
                                              nearest[0] +
                                              getEndingNearest(nearest[0]),
                                            super_text:
                                              nearest[1] +
                                              getEndingNearest(nearest[1]),
                                            top_id: nearest[2],
                                            super_id: nearest[3],
                                          },
                                        });

                                        //Pushing to accelera
                                        accelera
                                          .publishTrigger(
                                            req.body.profile_id,
                                            "Tabby-attempt",
                                            {
                                              game_id: req.body.game.game_id,
                                              profile_id: req.body.profile_id,
                                              player_id: req.body.player_id,
                                              reward: personalized_reward,
                                              sms_type:
                                                personalized_reward.sms_type,
                                              remain: remain,
                                              last_step_uuid: last_step_uuid,
                                              ip: req.body.request_ip,
                                            }
                                          )
                                          .then(function () {
                                            log.info(
                                              "Trigger was published:",
                                              "Tabby-attempt",
                                              req.body.profile_id,
                                              result
                                            );
                                          })
                                          .catch((e) => {
                                            log.error(
                                              "Failed to publish trigger:",
                                              e
                                            );
                                          });
                                      }
                                    );

                                    //Check for collection element
                                    if (
                                      personalized_reward.id.includes("c-") ===
                                      true
                                    ) {
                                      //Its a collection element
                                      //Getting achievements (collection) to define size for event
                                      Achievements.findbyprofile(
                                        req,
                                        function (err, collection) {
                                          if (err) {
                                            log.error(
                                              "Cannot get connection after created new element:",
                                              req.body.profile_id,
                                              err
                                            );
                                          } else {
                                            let group = _.groupBy(
                                              collection,
                                              "category"
                                            );
                                            let c_arr =
                                              personalized_reward.id.split("-");
                                            let coll_category =
                                              c_arr[0] + "-" + c_arr[1];
                                            let achievement =
                                              group[coll_category] === undefined
                                                ? []
                                                : group[coll_category].map(
                                                    function (o) {
                                                      return o.id;
                                                    }
                                                  );
                                            let new_one = [
                                              personalized_reward.id,
                                            ];
                                            let array_with_new = [
                                              ...new Set([
                                                ...achievement,
                                                ...new_one,
                                              ]),
                                            ];
                                            let collection_len =
                                              array_with_new.length;
                                            let target_len =
                                              req.body.game.collections[
                                                coll_category
                                              ]["length"];

                                            let collection_final_gift = _.find(
                                              req.body.game.rewards,
                                              { id: coll_category }
                                            );
                                            collection_final_gift[
                                              "creation_date"
                                            ] = moment(new Date()).format(
                                              "DD/MM/YYYY"
                                            );
                                            //Pushing to accelera
                                            accelera
                                              .publishTrigger(
                                                req.body.profile_id,
                                                "Tabby-collection",
                                                {
                                                  game_id:
                                                    req.body.game.game_id,
                                                  profile_id:
                                                    req.body.profile_id,
                                                  player_id: req.body.player_id,
                                                  element:
                                                    personalized_reward.id,
                                                  category: coll_category,
                                                  target: target_len,
                                                  actual: collection_len,
                                                  reward: collection_final_gift,
                                                }
                                              )
                                              .then(function () {
                                                log.info(
                                                  "Trigger was published:",
                                                  "Tabby-collection",
                                                  req.body.profile_id,
                                                  JSON.stringify({
                                                    game_id:
                                                      req.body.game.game_id,
                                                    profile_id:
                                                      req.body.profile_id,
                                                    player_id:
                                                      req.body.player_id,
                                                    element:
                                                      personalized_reward.id,
                                                    category: coll_category,
                                                    target: target_len,
                                                    actual: collection_len,
                                                    reward:
                                                      collection_final_gift,
                                                  })
                                                );
                                              })
                                              .catch((e) => {
                                                log.error(
                                                  "Failed to publish trigger:",
                                                  e
                                                );
                                              });
                                          }
                                        }
                                      );
                                    }
                                  } else {
                                    log.error(
                                      "There is no coupon or partner is not-active:"
                                    );
                                    send(res, 200, {
                                      status: "no_coupons_left",
                                      step: step,
                                      position: next_position,
                                      counters: req.body.counters,
                                      balance: parseInt(changes.balance),
                                      ending:
                                        changes.balance !== "0"
                                          ? getEnding(changes.balance)
                                          : getEnding(1),
                                      gifts: [personalized_reward],
                                      is_finished: false,
                                      same_elements: false,
                                      same_promocode: false,
                                      promocode: "",
                                      remain: 0,
                                      limit: limit,
                                      timer_active: timer_active,
                                      next_tries: next_tries,
                                      time_now: intervals.time_now,
                                      time_end: intervals.next_tick,
                                      active_timer_rewards:
                                        active_timer_rewards,
                                      nearest_prizes: {
                                        top_text:
                                          nearest[0] +
                                          getEndingNearest(nearest[0]),
                                        super_text:
                                          nearest[1] +
                                          getEndingNearest(nearest[1]),
                                        top_id: nearest[2],
                                        super_id: nearest[3],
                                      },
                                    });
                                  }
                                }
                              );
                            } else {
                              //Already have
                              let existing_reward = issued[result.id];
                              let updatedlink = decodeHTMLEntities(
                                existing_reward.link
                              );

                              //Update existing description
                              let gamerewards = _.find(req.body.game.rewards, {
                                id: result.id,
                              });
                              existing_reward.full_description =
                                gamerewards.full_description;
                              existing_reward.title = gamerewards.title;
                              existing_reward.short_description =
                                gamerewards.short_description;
                              existing_reward.link = updatedlink;
                              //New for time limited rewards
                              existing_reward.images = gamerewards.images;

                              //Fix for benny promocode
                              //existing_reward.promocode = (existing_reward.promocode === 'BEENY202' ) ? "BEENY2023" : existing_reward.promocode;

                              send(res, 200, {
                                status: "ok",
                                step: step,
                                position: next_position,
                                counters: req.body.counters,
                                balance: parseInt(changes.balance),
                                ending:
                                  changes.balance !== "0"
                                    ? getEnding(changes.balance)
                                    : getEnding(1),
                                gifts: [_.cloneDeep(existing_reward)],
                                is_finished: false,
                                same_elements: false,
                                same_promocode: true,
                                promocode: "",
                                limit: limit,
                                timer_active: timer_active,
                                next_tries: next_tries,
                                time_now: intervals.time_now,
                                time_end: intervals.next_tick,
                                active_timer_rewards: active_timer_rewards,
                                nearest_prizes: {
                                  top_text:
                                    nearest[0] + getEndingNearest(nearest[0]),
                                  super_text:
                                    nearest[1] + getEndingNearest(nearest[1]),
                                  top_id: nearest[2],
                                  super_id: nearest[3],
                                },
                              });

                              Rewards.create(
                                { body: _.cloneDeep(existing_reward) },
                                function (err, ok) {}
                              );
                              //Pushing to accelera as existing gift
                              // existing_reward.coupon = existing_reward.promocode;
                              // accelera.publishTrigger(req.body.profile_id, "Tabby-attempt", {
                              //     "game_id" : req.body.game.game_id,
                              //     "profile_id" : req.body.profile_id,
                              //     "player_id" : req.body.player_id,
                              //     "reward" : _.cloneDeep(existing_reward)
                              // }).then(function (){
                              //     log.info('Trigger was published:', "Tabby-attempt", req.body.profile_id, result);
                              // }).catch(e => {
                              //     log.error('Failed to publish trigger:', e);
                              // });
                            }
                          });
                        } else {
                          //Its a top reward which user have to choose
                          Profile.getTabbyTopRemain(
                            result,
                            function (err, remain) {
                              delete result["promocode"];
                              result.remain = remain;

                              send(res, 200, {
                                status: remain !== 0 ? "ok" : "no_coupons_left",
                                step: step,
                                position: next_position,
                                counters: req.body.counters,
                                balance: parseInt(changes.balance),
                                ending:
                                  changes.balance !== "0"
                                    ? getEnding(changes.balance)
                                    : getEnding(1),
                                gifts: [_.cloneDeep(result)],
                                is_finished: false,
                                same_elements: false,
                                same_promocode: false,
                                promocode: "",
                                remain: remain,
                                limit: limit,
                                timer_active: timer_active,
                                next_tries: next_tries,
                                time_now: intervals.time_now,
                                time_end: intervals.next_tick,
                                active_timer_rewards: active_timer_rewards,
                                nearest_prizes: {
                                  top_text:
                                    nearest[0] + getEndingNearest(nearest[0]),
                                  super_text:
                                    nearest[1] + getEndingNearest(nearest[1]),
                                  top_id: nearest[2],
                                  super_id: nearest[3],
                                },
                              });
                            }
                          );
                        }
                      });
                    });

                    //Storing points to user ID (leaderboard by user)
                    if (result.id.includes("r-") === true) {
                      let value = result.id.split("-");
                      Leaderboard.increase(
                        {
                          body: {
                            system: req.body.game_id,
                            name: "points",
                            value: value[1] * multiply_rating,
                            profile_id: req.body.player_id,
                          },
                        },
                        function (err) {
                          if (err) {
                            log.error(
                              "Failed to reload leaderboard to:",
                              req.body.profile_id,
                              err
                            );
                          } else {
                            log.info(
                              "Leaderboard is updated",
                              req.body.profile_id,
                              "points",
                              value[1],
                              "/ multiply rating",
                              multiply_rating
                            );
                          }
                        }
                      );
                    }
                  }

                  //Update analytics
                  let event = {
                    event: "accelera-api",
                    jwt: req.body.token,
                    page: "map",
                    status: "step",
                    additional: last_step_uuid,
                    game_id: req.body.game.game_id,
                    details: step.toString(),
                    gifts: [
                      position.toString(),
                      type_cell,
                      next_position.toString(),
                      next_on_map.COUNTERVALUE,
                    ],
                    profile_id: req.body.profile_id,
                    player_id:
                      req.body.player_id === undefined
                        ? ""
                        : req.body.player_id.toString(),
                    timestamp: Math.floor(new Date()),
                    date: moment(new Date()).format("YYYY-MM-DD"),
                    time: moment(new Date()).format("HH:mm"),
                    datetime: moment(
                      momentTimezone.tz("Europe/Moscow")._d
                    ).format("YYYY-MM-DD HH:mm:ss"),
                  };

                  bulk.store(
                    req.body.game.game_id,
                    JSON.stringify(event),
                    function (err) {
                      if (err) {
                        log.error(
                          "Error while storing webhooks messages for Clickhouse bulk:",
                          err
                        );
                      }
                    }
                  );
                }
              });

              function decodeHTMLEntities(text) {
                if (typeof text === "string") {
                  let entities = [
                    ["#95", "_"],
                    ["#x3D", "="],
                    ["amp", "&"],
                    ["apos", "'"],
                    ["#x27", "'"],
                    ["#x2F", "/"],
                    ["#39", "'"],
                    ["#47", "/"],
                    ["lt", "<"],
                    ["gt", ">"],
                    ["nbsp", " "],
                    ["quot", '"'],
                    ["quote", '"'],
                    ["#39", "'"],
                    ["#34", '"'],
                  ];

                  for (let i in entities) {
                    let toreplace = "&" + entities[i][0] + ";";
                    text = text.replace(
                      new RegExp(toreplace, "g"),
                      entities[i][1]
                    );
                  }

                  return text;
                } else {
                  return text;
                }
              }

              function processGift(done) {
                switch (next_on_map.COUNTERVALUE) {
                  case "0": {
                    log.info("No gift on the next step");
                    let move = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "position",
                      value: step,
                    };

                    Counter.modify({ body: move }, function (err, done) {});

                    return send(res, 200, {
                      status: "ok",
                      step: step,
                      position: next_position,
                      counters: req.body.counters,
                      balance: parseInt(changes.balance),
                      ending:
                        changes.balance !== "0"
                          ? getEnding(changes.balance)
                          : getEnding(1),
                      gifts: [
                        {
                          id: "c-2-2",
                          title: "Тут приза нет",
                          short_description: "Продолжай свой путь к призам!",
                          activation_type: "empty",
                        },
                      ],
                      is_finished: false,
                      same_elements: false,
                      same_promocode: false,
                      promocode: "",
                      remain: 0,
                      limit: limit,
                      timer_active: timer_active,
                      next_tries: next_tries,
                      time_now: intervals.time_now,
                      time_end: intervals.next_tick,
                      active_timer_rewards: active_timer_rewards,
                      nearest_prizes: {
                        top_text: nearest[0] + getEndingNearest(nearest[0]),
                        super_text: nearest[1] + getEndingNearest(nearest[1]),
                        top_id: nearest[2],
                        super_id: nearest[3],
                      },
                    });
                  }

                  case "freezed": {
                    log.info("Gift will be freezed portal");
                    let gift = req.body.game.rewards.find(
                      (item) => item.id === "freezed"
                    );
                    gift.icon = gift.id;
                    done(null, gift);
                    break;
                  }
                  case "portal": {
                    log.info("Gift will be portal");
                    let gift = req.body.game.rewards.find(
                      (item) => item.id === "portal"
                    );
                    gift.icon = gift.id;
                    done(null, gift);
                    break;
                  }

                  case "treasure_partner": {
                    //TODO delete it, just for demo of time limited gift
                    //let sample = _.sample(["x-61", "x-64", "x-66", "x-67", "x-68", "x-69", "x-70", "x-71"]);
                    /*let sample = _.sample(["s-5"]);

                                    let gift = req.body.game.rewards.find(item => item.id === sample);
                                    gift.icon = gift.id;
                                    done(null, gift);
                                    break;*/
                    //Algorythm 3
                    let treasure_partner_unfiltered =
                      req.body.personal_partners;

                    //Отбираем только активные партнеры чтобы отфильтровать персональный список
                    let j = _.filter(
                      Object.values(req.body.game.rewards),
                      function (o) {
                        return o.status !== "active";
                      }
                    ).map(function (j) {
                      return j.id;
                    });

                    let treasure_partner = treasure_partner_unfiltered.filter(
                      function (el) {
                        return j.indexOf(el) < 0;
                      }
                    );

                    log.info(
                      "Filtered personal partners are:",
                      treasure_partner
                    );
                    // let rate = treasure_partner.length > 1 ? 75 : treasure_partner.length === 0 ? 0 : treasure_partner.length * 10 + 10;
                    let rate =
                      treasure_partner.length === 0
                        ? 0
                        : treasure_partner.length * 10 + 10 > 75
                        ? 75
                        : treasure_partner.length * 10 + 10;

                    let random = Math.floor(Math.random() * 100) + 1; //1-100

                    if (random <= rate) {
                      //Partner personal reward
                      let sample = _.sample(treasure_partner);
                      log.info("Gift will be sampled:", sample);
                      let gift = req.body.game.rewards.find(
                        (item) => item.id === sample
                      );
                      gift.icon = gift.id;
                      done(null, gift);
                      break;
                    } else {
                      //Algorythm 1
                      let id_partner = _.filter(
                        req.body.game.rewards,
                        function (v) {
                          //return (v.type === 'partners' && (v.prize_type === 'special' || v.prize_type === 'gifts') && v.status === 'active');
                          //return (v.type === 'partners' && v.prize_type === 'gifts' && v.status === 'active');
                          // return (v.type === 'partners' && (v.prize_type === 'map' || v.prize_type === 'gifts') && v.status === 'active');
                          return (
                            v.type === "partners" &&
                            (random > 20 || v.prize_type === "gifts"
                              ? v.prize_type === "gifts"
                              : v.prize_type === "map") &&
                            v.status === "active"
                          );
                        }
                      );

                      let filtered = _.sample(id_partner);
                      log.info("Gift will be:", filtered.id);
                      let gift = req.body.game.rewards.find(
                        (item) => item.id === filtered.id
                      );
                      gift.icon = gift.id;
                      console.log(gift.id);
                      done(null, gift);
                      break;
                    }
                  }

                  case "treasure_ratings": {
                    //Algorythm 2
                    let random = Math.floor(Math.random() * 9999) + 1; //1-10000
                    let filtered = _.filter(
                      req.body.game.ratings,
                      function (item) {
                        return random >= item.from && random <= item.to;
                      }
                    );

                    log.info("Gift will be:", filtered[0].id);
                    let gift = req.body.game.rewards.find(
                      (item) => item.id === filtered[0].id
                    );
                    gift.icon = gift.id;
                    done(null, gift);
                    break;
                  }

                  default: {
                    log.warn("Default gift will be:", next_on_map.COUNTERVALUE);
                    let gift = req.body.game.rewards.find(
                      (item) => item.id === next_on_map.COUNTERVALUE
                    );
                    gift.icon = gift.id;
                    done(null, gift);
                    break;
                  }
                }
              }
            }
          } else {
            send(res, 500, {
              status: "not_enough_balance",
              step: 0,
              position: position,
              balance: 0,
              counters: req.body.counters,
              ending: getEnding(0),
              gifts: [],
              is_finished: false,
              same_elements: false,
              same_promocode: false,
              promocode: "",
              limit: limit,
              timer_active: timer_active,
              next_tries: next_tries,
              time_now: intervals.time_now,
              time_end: intervals.next_tick,
              active_timer_rewards: active_timer_rewards,
              nearest_prizes: {
                top_text: nearest[0] + getEndingNearest(nearest[0]),
                super_text: nearest[1] + getEndingNearest(nearest[1]),
                top_id: nearest[2],
                super_id: nearest[3],
              },
            });
          }
        });
      } else {
        send(res, 500, {
          status: "not_enough_balance",
          step: 0,
          position: position,
          balance: 0,
          counters: req.body.counters,
          ending: getEnding(1),
          gifts: [],
          is_finished: false,
          same_elements: false,
          same_promocode: false,
          promocode: "",
          limit: limit,
          timer_active: timer_active,
          next_tries: next_tries,
          time_now: intervals.time_now,
          time_end: intervals.next_tick,
          active_timer_rewards: active_timer_rewards,
          nearest_prizes: {
            top_text: nearest[0] + getEndingNearest(nearest[0]),
            super_text: nearest[1] + getEndingNearest(nearest[1]),
            top_id: nearest[2],
            super_id: nearest[3],
          },
        });
      }
    });

    function getEnding(num) {
      let last = num.toString().slice(-1);
      let ord = "";

      switch (last) {
        case "1":
          if (num.toString() === "111") {
            ord = " бросков";
          } else {
            ord = " бросок";
          }
          break;
        case "2":
          ord = " броска";
          break;
        case "3":
          ord = " броска";
          break;
        case "4":
          ord = " броска";
          break;
        default:
          ord = " бросков";
          break;
      }

      return ord;
    }

    function getEndingNearest(num) {
      let last = num.toString().slice(-1);
      let ord = "";

      switch (last) {
        case "1":
          if (num.toString() === "111") {
            ord = " клеток";
          } else {
            ord = " клетка";
          }
          break;
        case "2":
          ord = " клетки";
          break;
        case "3":
          ord = " клетки";
          break;
        case "4":
          ord = " клетки";
          break;
        default:
          ord = " клеток";
          break;
      }

      return ord;
    }
  }
);

router.post(
  "/rewards",
  passport.authenticate("api", { session: false }),
  birthdayLimiter,
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    let out_rewards = _.filter(req.body.game.rewards, function (g) {
      delete g["btn_name"];
      delete g["prize_type"];
      delete g["share_with_friend"];
      delete g["promocode"];
      delete g["sms_type"];
      delete g["sms_1"];
      delete g["sms_2"];
      delete g["activation_type"];
      g.priority = g.priority === undefined ? 999 : g.priority;
      g.icon = g.id.replace("total-", "");
      return (
        (g.id.includes("top-") === true ||
          g.id.includes("x-") === true ||
          [
            "c-1",
            "c-2",
            "c-3",
            "c-4",
            "c-5",
            "c-6",
            "c-7",
            "c-8",
            "c-9",
            "c-10",
            "c-11",
            "c-12",
            "c-13",
            "c-14",
            "c-15",
            "c-16",
            "c-17",
          ].includes(g.id) === true) &&
        ["active", "archived"].includes(g.status) === true
      );
    });
    let out_history = [];
    let archive = [];
    let currentTime = Math.floor(new Date());
    let unarchived = [];

    _.forEach(out_rewards, function (reward) {
      if (reward.archive_from !== undefined) {
        if (reward.archive_from <= currentTime) {
          archive.push(reward);
        } else {
          unarchived.push(reward);
        }
      } else {
        unarchived.push(reward);
      }
    });

    Rewards.findbyprofile(req, function (err, rewards) {
      if (err) {
        send(res, 500, {
          status: "failed",
          rewards: [],
          history: [],
          archive: [],
        });
      } else {
        let i = 0;
        _.forEach(rewards, function (value, key) {
          if (value.id === undefined) {
            log.error("Undefined reward:", value, req.body.profile_id);
            i++;
          } else {
            if (
              value.id.includes("r-") !== true &&
              value.id.includes("c-1-") !== true &&
              value.id.includes("c-2-") !== true &&
              value.id.includes("c-3-") !== true &&
              value.id.includes("c-4-") !== true &&
              value.id.includes("c-5-") !== true &&
              value.id.includes("c-6-") !== true &&
              value.id.includes("c-7-") !== true &&
              value.id.includes("c-8-") !== true &&
              value.id.includes("c-9-") !== true &&
              value.id.includes("c-10-") !== true &&
              value.id.includes("c-11-") !== true &&
              value.id.includes("c-12-") !== true &&
              value.id.includes("c-13-") !== true &&
              value.id.includes("c-14-") !== true &&
              value.id.includes("c-15-") !== true &&
              value.id.includes("c-16-") !== true &&
              value.id.includes("c-17-") !== true &&
              value.id.includes("c-18-") !== true &&
              value.id.includes("c-19-") !== true &&
              value.id.includes("c-20-") !== true &&
              value.id.includes("c-21-") !== true &&
              value.id.includes("c-22-") !== true &&
              value.id.includes("c-23-") !== true &&
              value.id.includes("c-24-") !== true &&
              value.id.includes("c-25-") !== true &&
              value.id.includes("c-26-") !== true &&
              value.id.includes("c-27-") !== true
            ) {
              value.id = value.id.replace("total-", "");
              let gamerewards = _.find(req.body.game.rewards, { id: value.id });
              if (gamerewards === undefined)
                log.error(
                  "Reward was not found!",
                  value.id,
                  req.body.profile_id,
                  value
                );
              value.full_description =
                value.activation_type === "discount"
                  ? gamerewards.full_description_2
                  : gamerewards.full_description;
              value.icon =
                gamerewards.id.length < 10 ? gamerewards.id : value.icon;
              value.title = gamerewards.title;
              value.short_description = gamerewards.short_description;
              value.service_code = gamerewards.service_code;
              //value.activation_type = gamerewards.activation_type;

              //New for time limited rewards
              value.images = gamerewards.images;
              value.is_bought =
                value.is_bought === undefined
                  ? false
                  : value.is_bought !== "false";
              let time_end = 0;

              if (value.activation_type === "discount") {
                let created_timestamp = parseInt(value.created_timestamp);
                let seconds_to_archive = parseInt(value.seconds_to_archive);
                let now = Math.floor(new Date());
                let end = created_timestamp + seconds_to_archive * 1000;
                value.time_end = now > end ? 0 : Math.round((end - now) / 1000);
                time_end = value.time_end;
              }

              if (gamerewards.link.includes("{{promocode}}") === false) {
                value.link = decodeHTMLEntities(gamerewards.link);
                value.link_desc = gamerewards.link_desc;
                value.link2 = decodeHTMLEntities(gamerewards.link2);
                value.link2_desc = gamerewards.link2_desc;
              }
              if (gamerewards.id === "x-2") {
                value.activation_type = "unique";
              }

              if (time_end === 0 && value.activation_type === "discount") {
                archive.push(value);
              } else {
                //last check for archived
                if (gamerewards.status !== "archived") {
                  out_history.push(_.cloneDeep(value));
                }
              }
              //out_history.push(_.cloneDeep(value));
              i++;
            } else {
              i++;
            }
          }
        });

        if (i === _.size(rewards)) {
          send(res, 200, {
            status: "ok",
            rewards: _.orderBy(
              _.filter(unarchived, function (v) {
                return v.priority !== 0;
              }),
              ["priority"],
              ["asc"]
            ),
            history: _.orderBy(out_history, ["timestamp"], ["desc"]),
            archive: _.orderBy(
              _.filter(archive, function (v) {
                return v.priority !== 0;
              }),
              ["priority"],
              ["asc"]
            ),
          });
        }
      }
    });

    function decodeHTMLEntities(text) {
      if (typeof text === "string") {
        let entities = [
          ["#95", "_"],
          ["#x3D", "="],
          ["amp", "&"],
          ["apos", "'"],
          ["#x27", "'"],
          ["#x2F", "/"],
          ["#39", "'"],
          ["#47", "/"],
          ["lt", "<"],
          ["gt", ">"],
          ["nbsp", " "],
          ["quot", '"'],
          ["quote", '"'],
          ["#39", "'"],
          ["#34", '"'],
        ];

        for (let i in entities) {
          let toreplace = "&" + entities[i][0] + ";";
          text = text.replace(new RegExp(toreplace, "g"), entities[i][1]);
        }

        return text;
      } else {
        return text;
      }
    }
  }
);

router.post(
  "/invites",
  passport.authenticate("api", { session: false }),
  API.getGame,
  (req, res, next) => {
    send(res, 200, {});

    log.warn("  [!] Got new invite request!", req.body.invited_by);

    //Getting level settings
    redis.hget(
      "platform:birthday:optins",
      req.body.invited_by,
      function (err, levels) {
        let parsedlevels = JSON.parse(levels);
        log.warn("Levels for CTN are found:", parsedlevels.ctn);

        Profile.findbyuser(
          {
            body: {
              system: req.body.game.game_id,
              id: "7" + parsedlevels.ctn.toString(),
            },
          },
          function (err, profile) {
            //Getting profile rewards
            Rewards.findbyprofile(
              { body: { profile_id: profile.profile_id } },
              function (err, rewards) {
                //log.info('Checking personal levels & rewards, enrich them with gifts:', parsedlevels);
                let gotoB = [
                  "task_2_place1",
                  "task_2_place2",
                  "task_2_place3",
                  "task_2_place4",
                  "task_2_place5",
                  "task_2_place6",
                ];

                for (let i in gotoB) {
                  //Sending session result event to a flow
                  if (parsedlevels[gotoB[i]] !== "") {
                    let reward = _.find(req.body.game.rewards, {
                      id: parsedlevels[gotoB[i]],
                    });
                    if (Object.keys(rewards).includes(reward.id) === false) {
                      //This is a gift we didnt give
                      log.warn(
                        "  [!] Issue a gift for invite friend:",
                        reward.id,
                        gotoB[i]
                      );

                      Profile.getBirthdayCoupon(
                        reward,
                        function (err, personalized_reward) {
                          if (!err) {
                            //Pushing to accelera
                            accelera
                              .publishTrigger(
                                profile.profile_id,
                                "invite-reward",
                                {
                                  game_id: req.body.game.game_id,
                                  profile_id: profile.profile_id,
                                  player_id: "7" + parsedlevels.ctn.toString(),
                                  reward: personalized_reward,
                                }
                              )
                              .then(function () {
                                log.warn(
                                  "Trigger was published:",
                                  "invite-reward",
                                  profile.profile_id,
                                  personalized_reward
                                );
                              })
                              .catch((e) => {
                                log.error("Failed to publish trigger:", e);
                              });
                          }
                        }
                      );
                    }
                  }
                }
              }
            );
          }
        );
      }
    );
  }
);

router.post(
  "/leaderboard",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    req.body.name = "points";
    Leaderboard.get(req, function (err, leaderboard) {
      if (err) return send(res, 500, { status: "failed" });
      send(res, 200, {
        status: "ok",
        rating: leaderboard,
      });
    });
  }
);

router.post(
  "/leaderboard/rewards",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    //Reformat leaderboard daily/weekly/monthly data
    let daily_reformatted = [];
    for (let i in req.body.game.leaderboard.gifts.daily) {
      if (typeof req.body.game.leaderboard.gifts.daily[i].place === "string") {
        reformat(
          req.body.game.leaderboard.gifts.daily[i].place.split("-")[0],
          req.body.game.leaderboard.gifts.daily[i].place.split("-")[1],
          req.body.game.leaderboard.gifts.daily[i]
        );
      } else {
        daily_reformatted.push(req.body.game.leaderboard.gifts.daily[i]);
      }
    }

    //Reformat leaderboard daily/weekly/monthly data
    let weekly_reformatted = [];
    for (let i in req.body.game.leaderboard.gifts.weekly) {
      if (typeof req.body.game.leaderboard.gifts.weekly[i].place === "string") {
        reformat_weekly(
          req.body.game.leaderboard.gifts.weekly[i].place.split("-")[0],
          req.body.game.leaderboard.gifts.weekly[i].place.split("-")[1],
          req.body.game.leaderboard.gifts.weekly[i]
        );
      } else {
        weekly_reformatted.push(req.body.game.leaderboard.gifts.weekly[i]);
      }
    }

    //Reformat leaderboard daily/weekly/monthly data
    let monthly_reformatted = [];
    for (let i in req.body.game.leaderboard.gifts.monthly) {
      if (
        typeof req.body.game.leaderboard.gifts.monthly[i].place === "string"
      ) {
        reformat_monthly(
          req.body.game.leaderboard.gifts.monthly[i].place.split("-")[0],
          req.body.game.leaderboard.gifts.monthly[i].place.split("-")[1],
          req.body.game.leaderboard.gifts.monthly[i]
        );
      } else {
        monthly_reformatted.push(req.body.game.leaderboard.gifts.monthly[i]);
      }
    }

    function reformat(from, to, data) {
      for (let i = parseInt(from); i <= parseInt(to); i++) {
        let newdata = _.cloneDeep(data);
        newdata.place = i;
        daily_reformatted.push(newdata);
      }
    }

    function reformat_weekly(from, to, data) {
      for (let i = parseInt(from); i <= parseInt(to); i++) {
        let newdata = _.cloneDeep(data);
        newdata.place = i;
        weekly_reformatted.push(newdata);
      }
    }

    function reformat_monthly(from, to, data) {
      for (let i = parseInt(from); i <= parseInt(to); i++) {
        let newdata = _.cloneDeep(data);
        newdata.place = i;
        monthly_reformatted.push(newdata);
      }
    }

    //Reply
    req.body.game.leaderboard.gifts.daily = daily_reformatted;
    req.body.game.leaderboard.gifts.weekly = weekly_reformatted;
    req.body.game.leaderboard.gifts.monthly = monthly_reformatted;

    send(res, 200, {
      status: "ok",
      leaderboard: req.body.game.leaderboard,
    });
  }
);

router.post(
  "/collections",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.AcceleraCoupons,
  (req, res, next) => {
    let collections = req.body.game.collections;
    let archive = [];
    let currentTime = Math.floor(new Date());

    Achievement.findbyprofile(req, function (err, achievements) {
      let achievements_group = _.groupBy(achievements, "category");
      let collections_arr = [];

      let j = 0;

      for (let i in collections) {
        let item = collections[i];
        item.remain =
          req.body.game.collections[item.id].accelera_stack !== undefined
            ? req.body.accelera_coupons[
                req.body.game.collections[item.id].accelera_stack
              ].size
            : 0;

        item.achievements =
          achievements_group[item.id] === undefined
            ? 0
            : achievements_group[item.id].length;
        item.items = [{}];

        _.forEach(achievements, function (value, key) {
          if (key.includes(item.id) === true) {
            item.items[0][value.id] = value;
          }
        });

        if (item.archive_from <= currentTime) {
          item.name = item.topic;
          archive.push(item);
          j++;
        } else {
          collections_arr.push(item);
          j++;
        }

        if (j === Object.keys(collections).length) {
          send(res, 200, {
            status: "ok",
            collections: collections_arr,
            archive: archive,
          });
        }
      }
    });
  }
);

// router.post('/collections/get', passport.authenticate('api', { session: false}), API.getGame, Token.Decrypt, (req, res, next) => {
//     let collections = req.body.game.collections;
//     let to_give = collections[req.body.collection]["reward"]; // ex.top-10 to get descriptions
//     let gift = req.body.game.rewards.find(item => item.id === to_give); // descriptions of the gift
//     gift.icon = gift.id;
//
//     Achievement.findbyprofile(req, function (err, achievements) {
//         let achievements_group = _.groupBy(achievements, "category");
//         log.info('Group is',achievements_group)
//         if (achievements_group[req.body.collection] !== undefined) {
//             if (Object.keys(achievements_group[req.body.collection]).length === collections[req.body.collection]["length"]) {
//                 //Collected all elements
//                 //Check if I already have this gift
//                 Rewards.findbyprofile(req, function (err, issued){
//                     let issued_partners = Object.keys(issued);
//
//                     if (issued_partners.includes(req.body.collection) === false) {
//                         //No such gift / can be issued
//                         Profile.getTabbyCollectionCoupon(req.body.collection, function (err, coupon){
//                             if (!err) {
//                                 gift.unique = "false";
//                                 gift.reason = "collection";
//                                 gift.promocode = coupon;
//                                 gift.coupon = coupon;
//                                 gift.game_id = 'Tabby';
//                                 gift.profile_id = req.body.profile_id;
//                                 gift.id = req.body.collection;
//                                 gift.link = decodeHTMLEntities(collections[req.body.collection]["link"]);
//                                 gift.full_description = collections[req.body.collection]["details"];
//
//                                 send(res, 200, {
//                                     "status" : "ok",
//                                     "gifts" : [
//                                         gift
//                                     ]
//                                 });
//
//                                 //Creating reward
//                                 Rewards.create({"body" : _.cloneDeep(gift)}, function (err, done){
//                                     log.info('Rewarded with collection gift')
//                                 })
//
//                                 //Pushing to accelera
//                                 accelera.publishTrigger(req.body.profile_id, "Tabby-collection", {
//                                     "game_id" : req.body.game.game_id,
//                                     "profile_id" : req.body.profile_id,
//                                     "player_id" : req.body.player_id,
//                                     "colelction" : req.body.collection,
//                                     "reward" : gift
//                                 }).then(function (){
//                                     log.info('Trigger was published:', "Tabby-collection", req.body.profile_id);
//                                 }).catch(e => {
//                                     log.error('Failed to publish trigger:', e);
//                                 });
//
//                             } else {
//                                 log.error('Trying to get a collection gift but no coupons left:', achievements_group[req.body.collection])
//                                 send(res, 200, {
//                                     "status" : "no_coupons_left",
//                                     "gifts" : []
//                                 });
//                             }
//                         })
//                     } else {
//                         //Already have
//                         let existing_reward = issued[req.body.collection];
//                         let updatedlink = decodeHTMLEntities(existing_reward.link);
//                         existing_reward.link = updatedlink;
//                         existing_reward.icon = existing_reward.id;
//
//                         send(res, 200, {
//                             "status" : "ok",
//                             "gifts" : [
//                                 _.cloneDeep(existing_reward)
//                             ]
//                         });
//
//                     }
//                 })
//
//             } else {
//                 //Not collected all elements
//                 log.error('Trying to get a collection gift but not collected:', achievements_group[req.body.game.collection])
//                 send(res, 200, {
//                     "status" : "no_coupons_left",
//                     "gifts" : []
//                 });
//             }
//         } else {
//             log.error('Trying to get a collection gift but not collected:', achievements_group[req.body.game.collection])
//             send(res, 200, {
//                 "status" : "no_coupons_left",
//                 "gifts" : []
//             });
//         }
//     })
//
//     function decodeHTMLEntities(text) {
//         if (typeof text === 'string') {
//             let entities = [
//                 ['#95','_'],
//                 ['#x3D', '='],
//                 ['amp', '&'],
//                 ['apos', '\''],
//                 ['#x27', '\''],
//                 ['#x2F', '/'],
//                 ['#39', '\''],
//                 ['#47', '/'],
//                 ['lt', '<'],
//                 ['gt', '>'],
//                 ['nbsp', ' '],
//                 ['quot', '"'],
//                 ['quote', '"'],
//                 ['#39', "'"],
//                 ['#34','"']
//             ];
//
//             for (let i in entities) {
//                 let toreplace = '&'+entities[i][0]+';';
//                 text = text.replace(new RegExp(toreplace, 'g'), entities[i][1])
//
//             }
//
//             return text;
//         } else {
//             return text;
//         }
//     }
//
// });

router.post(
  "/tasks",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Task.findbyprofile(req, function (err, task) {
      if (err)
        return send(res, 500, { status: "failed", active: [], completed: [] });

      let active = task.active === undefined ? [] : task.active;
      let completed = task.completed === undefined ? [] : task.completed;

      let active_replaced = [];
      let completed_replaced = [];

      let i = 0;
      _.forEach(active, function (value) {
        let global_details = _.find(req.body.game.tasks, { name: value.name });

        if (global_details !== undefined) {
          value.full_description =
            global_details.full_description !== undefined
              ? global_details.full_description
              : value.full_description;
          value.short_description =
            global_details.short_description !== undefined
              ? global_details.short_description
              : value.short_description;
          value.link_1_link =
            global_details.link_1_link !== undefined
              ? global_details.link_1_link
              : value.link_1_link;
          value.link_1_desc =
            global_details.link_1_desc !== undefined
              ? global_details.link_1_desc
              : value.link_1_desc;
          value.link_2_link =
            global_details.link_2_link !== undefined
              ? global_details.link_2_link
              : value.link_2_link;
          value.link_2_desc =
            global_details.link_2_desc !== undefined
              ? global_details.link_2_desc
              : value.link_2_desc;
          value.link_3_link =
            global_details.link_3_link !== undefined
              ? global_details.link_3_link
              : value.link_3_link;
          value.link_3_desc =
            global_details.link_3_desc !== undefined
              ? global_details.link_3_desc
              : value.link_3_desc;
          value.link_4_link =
            global_details.link_4_link !== undefined
              ? global_details.link_4_link
              : value.link_4_link;
          value.link_4_desc =
            global_details.link_4_desc !== undefined
              ? global_details.link_4_desc
              : value.link_4_desc;
          value.achievement = global_details.achievement;
          active_replaced.push(value);
        }
        i++;
      });

      if (i === active.length) {
        let j = 0;
        _.forEach(completed, function (value) {
          let global_details = _.find(req.body.game.tasks, {
            name: value.name,
          });

          value.full_description = global_details.full_description;
          value.short_description = global_details.short_description;
          value.achievement = global_details.achievement;
          completed_replaced.push(value);

          j++;
        });

        if (j === completed.length) {
          send(res, 200, {
            status: "ok",
            active: _.cloneDeep(active_replaced),
            completed: _.cloneDeep(completed_replaced),
          });
        }
      }
    });
  }
);

router.post(
  "/top/decision",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.getMapSettingsbyProfile,
  API.Counters,
  (req, res, next) => {
    //Check top gift decision "yes" / "no"
    let on_map = _.find(req.body.levels, function (l) {
      return l.COUNTERKEY === parseInt(req.body.counters.position);
    });

    let gift = _.find(req.body.game.rewards, function (g) {
      return g.id === on_map.COUNTERVALUE;
    });

    //Check if I'm on top
    if (gift.id.includes("top") === true) {
      if (req.body.decision === "yes") {
        //Decision is YES
        let move = {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
          name: "position",
          value: 0,
        };

        Counter.create({ body: move }, function (err, done) {
          log.info("Player accepted TOP gift", on_map, gift.id);
        });

        Profile.getTabbyCoupon(
          gift,
          function (err, personalized_reward, remain) {
            if (!err) {
              personalized_reward.promocode = personalized_reward.coupon;
              let updatedlink = utils.decodeHTMLEntities(
                personalized_reward.link
              );

              utils.makeShort(updatedlink, function (link) {
                //https://dvizh.beeline.ru/?x=
                personalized_reward.link = updatedlink;
                personalized_reward.shortlink = link;
                //Sending update
                send(res, 200, {
                  status: "ok",
                  position: 0,
                });

                //Pushing to accelera
                accelera
                  .publishTrigger(req.body.profile_id, "Tabby-attempt", {
                    game_id: req.body.game.game_id,
                    profile_id: req.body.profile_id,
                    player_id: req.body.player_id,
                    reward: personalized_reward,
                    sms_type: personalized_reward.sms_type,
                    tries_count: personalized_reward.tries_count,
                    remain: remain,
                    last_step_uuid: req.body.counters.last_step_uuid,
                  })
                  .then(function () {
                    log.info(
                      "Trigger was published:",
                      "Tabby-attempt",
                      req.body.profile_id,
                      gift.id
                    );
                  })
                  .catch((e) => {
                    log.error("Failed to publish trigger:", e);
                  });
              });
            } else {
              log.error("There is no coupon:");
              //Sending update as current position because no coupons
              send(res, 200, {
                status: "no_coupons_left",
                position: parseInt(req.body.counters.position),
              });
            }
          }
        );
      } else {
        //Decision in NO
        log.info("Player rejected TOP gift", on_map, gift);
        send(res, 200, {
          status: "ok",
          position: parseInt(req.body.counters.position),
        });
      }
    } else {
      log.error(
        "Cannot get TOP gift, player is not on top position",
        on_map,
        gift
      );
      send(res, 500, {
        status: "failed",
      });
    }
  }
);

router.post(
  "/shop",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  (req, res, next) => {
    let limit_booster =
      req.body.counters.limit_booster !== undefined
        ? JSON.parse(req.body.counters.limit_booster)
        : "";
    let rating_booster =
      req.body.counters.rating_booster !== undefined
        ? JSON.parse(req.body.counters.rating_booster)
        : "";
    let speed_booster =
      req.body.counters.speed_booster !== undefined
        ? JSON.parse(req.body.counters.speed_booster)
        : "";

    let current = Math.floor(new Date());
    //Updating a time
    if (limit_booster !== "" && limit_booster.expired_at > current) {
      let boosterIndex = req.body.game.shop.casts.findIndex(
        (obj) => obj.id === limit_booster.id
      );
      req.body.game.shop.casts[boosterIndex].time = Math.round(
        (limit_booster.expired_at - current) / 1000
      );
    }

    if (rating_booster !== "" && rating_booster.expired_at > current) {
      let boosterIndex = req.body.game.shop.rating.findIndex(
        (obj) => obj.id === rating_booster.id
      );
      req.body.game.shop.rating[boosterIndex].time = Math.round(
        (rating_booster.expired_at - current) / 1000
      );
    }

    if (speed_booster !== "" && speed_booster.expired_at > current) {
      let boosterIndex = req.body.game.shop.casts.findIndex(
        (obj) => obj.id === speed_booster.id
      );
      req.body.game.shop.casts[boosterIndex].time = Math.round(
        (speed_booster.expired_at - current) / 1000
      );
    }
    //Sending update as current position because no coupons
    send(res, 200, {
      status: "ok",
      casts: req.body.game.shop.casts,
      rating: req.body.game.shop.rating,
    });
  }
);

router.post(
  "/shop/purchase",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  (req, res, next) => {
    //Sending update as current position because no coupons
    let shop = req.body.game.shop.casts.concat(req.body.game.shop.rating);
    let booster = _.find(shop, { id: req.body.id });

    let rating_booster =
      req.body.counters.rating_booster !== undefined
        ? JSON.parse(req.body.counters.rating_booster)
        : "";
    let limit_booster =
      req.body.counters.limit_booster !== undefined
        ? JSON.parse(req.body.counters.limit_booster)
        : "";
    let speed_booster =
      req.body.counters.speed_booster !== undefined
        ? JSON.parse(req.body.counters.speed_booster)
        : "";
    let active_rating_booster = _.find(shop, { id: rating_booster.id });
    let active_limit_booster = _.find(shop, { id: limit_booster.id });
    let active_speed_booster = _.find(shop, { id: speed_booster.id });

    switch (booster.category) {
      case "rating": {
        if (rating_booster !== "") {
          send(res, 200, {
            status: "ok",
            modal: "reject",
            topic: "",
            description:
              "Подключить «" +
              booster.description +
              "» можно после окончания бустера «" +
              active_rating_booster.description +
              "»",
            id: req.body.id,
          });
        } else {
          send(res, 200, {
            status: "ok",
            modal: "confirm",
            topic: "Подключить?",
            description:
              "Подключить «" +
              booster.description +
              "» за " +
              booster.price +
              " руб.?",
            id: req.body.id,
          });
        }
        break;
      }

      case "limit": {
        if (limit_booster !== "") {
          send(res, 200, {
            status: "ok",
            modal: "confirm",
            topic: "Подключить?",
            description:
              "У вас уже есть увеличение ежедневного лимита бросков на +" +
              active_limit_booster.value +
              ". При покупке нового увеличения бросков, предыдущий будет отменен",
            id: req.body.id,
          });
        } else {
          send(res, 200, {
            status: "ok",
            modal: "confirm",
            topic: "Подключить?",
            description:
              "Подключить «" +
              booster.description +
              "» за " +
              booster.price +
              " руб.?",
            id: req.body.id,
          });
        }
        break;
      }

      case "speed": {
        if (speed_booster !== "") {
          send(res, 200, {
            status: "ok",
            modal: "reject",
            topic: "",
            description:
              "Подключить «" +
              booster.description +
              "» можно после окончания бустера «" +
              active_speed_booster.description +
              "»",
            id: req.body.id,
          });
        } else {
          send(res, 200, {
            status: "ok",
            modal: "confirm",
            topic: "Подключить?",
            description:
              "Подключить «" +
              booster.description +
              "» за " +
              booster.price +
              " руб.?",
            id: req.body.id,
          });
        }
        break;
      }
    }
  }
);

router.post(
  "/shop/confirm",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  (req, res, next) => {
    //Sending update as current position because no coupons
    if (req.body.decision === "accept") {
      let shop = req.body.game.shop.casts.concat(req.body.game.shop.rating);
      let booster = _.find(shop, { id: req.body.id });
      let purchased = Math.floor(new Date());
      let expired_at = purchased + booster.days * 86400000;

      let rating_booster =
        req.body.counters.rating_booster !== undefined
          ? JSON.parse(req.body.counters.rating_booster).id
          : "";
      let limit_booster =
        req.body.counters.limit_booster !== undefined
          ? JSON.parse(req.body.counters.limit_booster).id
          : "";
      let speed_booster =
        req.body.counters.speed_booster !== undefined
          ? JSON.parse(req.body.counters.speed_booster).id
          : "";

      let prev_booster = "";

      Pack.purchaseBooster(req, res, booster.service_code, function () {
        switch (booster.category) {
          case "limit": {
            prev_booster = limit_booster;

            let limit = {
              profile_id: req.body.profile_id,
              game_id: req.body.game.game_id,
              name: "daily_limit",
              value: 50 + booster.value,
            };

            Counter.create({ body: limit }, function (err, done) {
              let booster_opts = {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                name: "limit_booster",
                value: JSON.stringify({
                  id: booster.id,
                  days: booster.days,
                  value: booster.value,
                  purchased: purchased,
                  expired_at: expired_at,
                }),
              };

              //Removing portal options
              Counter.create({ body: booster_opts }, function (err, done) {
                send(res, 200, {
                  status: "ok",
                  casts: req.body.game.shop.casts,
                  rating: req.body.game.shop.rating,
                });
              });
            });

            break;
          }

          case "rating": {
            prev_booster = rating_booster;

            let rating = {
              profile_id: req.body.profile_id,
              game_id: req.body.game.game_id,
              name: "multiply_rating",
              value: booster.value,
            };

            Counter.create({ body: rating }, function (err, done) {
              let booster_opts = {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                name: "rating_booster",
                value: JSON.stringify({
                  id: booster.id,
                  days: booster.days,
                  value: booster.value,
                  steps: booster.steps,
                  purchased: purchased,
                  expired_at: expired_at,
                }),
              };

              //Create booster options
              Counter.create({ body: booster_opts }, function (err, done) {
                //Reloading steps
                let steps_opts = {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "steps_made",
                  value: 0,
                };
                Counter.create({ body: steps_opts }, function (err, done) {
                  send(res, 200, {
                    status: "ok",
                    casts: req.body.game.shop.casts,
                    rating: req.body.game.shop.rating,
                  });
                });
              });
            });

            break;
          }

          case "speed": {
            prev_booster = speed_booster;

            let limit = {
              profile_id: req.body.profile_id,
              game_id: req.body.game.game_id,
              name: "booster_speed",
              value: booster.multiplier,
            };

            Counter.create({ body: limit }, function (err, done) {
              let booster_opts = {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                name: "speed_booster",
                value: JSON.stringify({
                  id: booster.id,
                  days: booster.days,
                  value: booster.value,
                  purchased: purchased,
                  expired_at: expired_at,
                }),
              };

              //Removing portal options
              Counter.create({ body: booster_opts }, function (err, done) {
                send(res, 200, {
                  status: "ok",
                  casts: req.body.game.shop.casts,
                  rating: req.body.game.shop.rating,
                });
              });
            });

            break;
          }
        }

        //Update analytics
        let event = {
          event: "accelera-api",
          page: "shop",
          status: "booster-purchased",
          game_id: req.body.game.game_id,
          additional: req.body.counters.last_step_uuid,
          details: booster.id,
          context: "purchase & activation",
          gifts: [
            expired_at.toString(),
            moment().add(booster.days, "d").format("YYYYMMDD_HHmmss"),
            booster.category,
            prev_booster,
          ],
          profile_id: req.body.profile_id,
          player_id:
            req.body.player_id === undefined
              ? ""
              : req.body.player_id.toString(),
          timestamp: Math.floor(new Date()),
          date: moment(new Date()).format("YYYY-MM-DD"),
          time: moment(new Date()).format("HH:mm"),
          datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
            "YYYY-MM-DD HH:mm:ss"
          ),
        };

        bulk.store(
          req.body.game.game_id,
          JSON.stringify(event),
          function (err) {
            if (err) {
              log.error(
                "Error while storing webhooks messages for Clickhouse bulk:",
                err
              );
            }
          }
        );
      });
    } else {
      send(res, 200, {
        status: "ok",
        casts: req.body.game.shop.casts,
        rating: req.body.game.shop.rating,
      });
    }
  }
);

router.post(
  "/portal/decision",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.getMapSettingsbyProfile,
  API.Counters,
  (req, res, next) => {
    //метод принятия решения по порталу, тут приходит id кнопки от 0 до 4
    if (req.body.counters.portal_opts === undefined) {
      log.warn(
        "Parse portals:",
        req.body.profile_id,
        req.body.counters.portal_opts
      );
      send(res, 500, {
        status: "failed",
        gifts: [],
      });
    } else {
      let portal_opts = JSON.parse(req.body.counters.portal_opts);
      let option = portal_opts[req.body.id];

      let next_on_map = _.find(req.body.levels, function (l) {
        if (req.body.id === 0) {
        } else {
          return l.COUNTERKEY === option.tile_id;
        }
      });
      if (req.body.id === 4) {
        if (Number(req.body.counters.steps_stay_here) <= 0) {
          return send(res, 500, {
            status: "failed",
            gifts: [],
          });
        }
      }
      //Check if its a free option
      if (option.id !== "p-0") {
        //Paid option
        let details = _.find(req.body.game.shop.portals, { id: option.id });
        //req.body.id = option.id; //Только для асинхронных тем, проверить что стоимостьь меняется
        Pack.purchaseBooster(req, res, details.service_code, function (err) {
          processStep();
        });
      } else {
        processStep();
      }

      function processStep() {
        let move = {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
          name: "position",
          value: option.tile_id,
        };

        Counter.create({ body: move }, function (err, done) {
          let move_opts = {
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            name: "portal_opts",
          };

          //Removing portal options
          Counter.remove({ body: move_opts }, function (err, done) {
            //Check is position was greater then choosed
            if (
              parseInt(req.body.counters.position) > parseInt(option.tile_id)
            ) {
              //Also reload req.body.counters.steps_stay_here counter to 8 because return back
              let steps_stay_here = {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                name: "steps_stay_here",
                value: 10,
              };

              Counter.create({ body: steps_stay_here }, function (err, done) {
                send(res, 200, {
                  status: "ok",
                  gifts: [],
                });
              });

              accelera
                .publishTrigger(req.body.profile_id, "new-round", {
                  game_id: req.body.game.game_id,
                  profile_id: req.body.profile_id,
                  player_id: req.body.player_id,
                })
                .then(function () {
                  log.info(
                    "Trigger was published:",
                    "new-round",
                    req.body.profile_id
                  );
                })
                .catch((e) => {
                  log.error("Failed to publish trigger:", e);
                });
            } else {
              if (req.body.id === 4) {
                //choosed to stay here, decreasing stay here
                let steps_stay_here = {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "steps_stay_here",
                  value: -1,
                };

                Counter.modify({ body: steps_stay_here }, function (err, done) {
                  send(res, 200, {
                    status: "ok",
                    gifts: [],
                  });
                });
              } else {
                send(res, 200, {
                  status: "ok",
                  gifts: [],
                });
              }
            }
          });
        });

        //Update analytics

        let event = {
          event: "accelera-api",
          page: "portals",
          status: "teleported",
          game_id: req.body.game.game_id,
          additional: req.body.counters.last_step_uuid,
          details: (req.body.id + 1).toString(),
          gifts: [
            option.tile_id.toString(),
            next_on_map !== undefined ? next_on_map.COUNTERVALUE : "start",
            req.body.counters.round.toString(),
          ],
          profile_id: req.body.profile_id,
          player_id:
            req.body.player_id === undefined
              ? ""
              : req.body.player_id.toString(),
          timestamp: Math.floor(new Date()),
          date: moment(new Date()).format("YYYY-MM-DD"),
          time: moment(new Date()).format("HH:mm"),
          datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
            "YYYY-MM-DD HH:mm:ss"
          ),
        };

        bulk.store(
          req.body.game.game_id,
          JSON.stringify(event),
          function (err) {
            if (err) {
              log.error(
                "Error while storing webhooks messages for Clickhouse bulk:",
                err
              );
            }
          }
        );
      }
    }
  }
);

router.post(
  "/reward/purchase",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    //Sending time limited reward confirmation
    let reward = _.find(req.body.game.rewards, {
      service_code: req.body.service_code,
    });

    Profile.getTabbyTopRemain(reward, function (err, remain) {
      //Check remain
      if (remain > 0) {
        Pack.purchaseBooster(req, res, req.body.service_code, function (err) {
          Profile.getTabbyCoupon(
            reward,
            function (err, personalized_reward, remain2) {
              if (!err) {
                personalized_reward.promocode = personalized_reward.coupon;
                //Updating coupon
                personalized_reward.is_bought = true;
                personalized_reward.disclamer = personalized_reward.disclamer_2;
                personalized_reward.full_description =
                  personalized_reward.full_description_2;
                personalized_reward.btn_name =
                  reward.btn_name_2 === undefined
                    ? "Активировать"
                    : reward.btn_name_2;
                personalized_reward.coupon_text =
                  reward.coupon_text === undefined
                    ? "Теперь введите промокод при оплате на сайте"
                    : reward.coupon_text;
                personalized_reward.images = reward.images;
                personalized_reward.seconds_to_archive = 0;
                //To send promocode
                personalized_reward.sms_type = "2";

                //Pushing to accelera
                accelera
                  .publishTrigger(req.body.profile_id, "Tabby-attempt", {
                    game_id: req.body.game.game_id,
                    profile_id: req.body.profile_id,
                    player_id: req.body.player_id,
                    reward: personalized_reward,
                    sms_type: personalized_reward.sms_type,
                  })
                  .then(function () {
                    log.info(
                      "Trigger was published:",
                      "Tabby-attempt",
                      req.body.profile_id,
                      personalized_reward
                    );
                  })
                  .catch((e) => {
                    log.error("Failed to publish trigger:", e);
                  });

                send(res, 200, {
                  status: "ok",
                  gifts: [personalized_reward],
                });

                //Remained event
                accelera
                  .publishTrigger(
                    req.body.profile_id,
                    "time-limited-purchased",
                    {
                      game_id: req.body.game.game_id,
                      profile_id: req.body.profile_id,
                      player_id: req.body.player_id,
                      reward: reward.id,
                      remain: remain2,
                    }
                  )
                  .then(function () {
                    log.info(
                      "Trigger was published:",
                      "coupons-less-100",
                      req.body.profile_id
                    );
                  })
                  .catch((e) => {
                    log.error("Failed to publish trigger:", e);
                  });

                //Update analytics
                let event = {
                  event: "accelera-api",
                  page: "shop",
                  status: "booster-purchased",
                  game_id: req.body.game.game_id,
                  details: reward.id,
                  context: "purchase & activation",
                  gifts: [],
                  profile_id: req.body.profile_id,
                  player_id:
                    req.body.player_id === undefined
                      ? ""
                      : req.body.player_id.toString(),
                  timestamp: Math.floor(new Date()),
                  date: moment(new Date()).format("YYYY-MM-DD"),
                  time: moment(new Date()).format("HH:mm"),
                  datetime: moment(
                    momentTimezone.tz("Europe/Moscow")._d
                  ).format("YYYY-MM-DD HH:mm:ss"),
                };

                bulk.store(
                  req.body.game.game_id,
                  JSON.stringify(event),
                  function (err) {
                    if (err) {
                      log.error(
                        "Error while storing webhooks messages for Clickhouse bulk:",
                        err
                      );
                    }
                  }
                );
              } else {
                log.error(
                  "Got error while purchasing a discounted reward",
                  req.body.service_code,
                  reward.id,
                  remain2
                );
                send(res, 200, {
                  status: "К сожалению, промокодов больше не осталось",
                  modal: "end",
                  gifts: [],
                });
              }
            }
          );
        });
      } else {
        log.error(
          "Got error while purchasing a discounted reward: no coupons left",
          req.body.service_code,
          reward.id,
          remain
        );
        send(res, 200, {
          status: "К сожалению, промокодов больше не осталось",
          modal: "end",
          gifts: [],
        });
      }
    });
  }
);

//Начисление сот
router.post(
  "/sot",
  passport.authenticate("management", { session: false }),
  API.getGame,
  Pack.sendSotActivation,
  (req, res, next) => {
    //Sending update as current position because no coupons
  }
);

//DEBTS PAYMENTS
router.post(
  "/debt/repay",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  Pack.payCreditdebts,
  (req, res, next) => {
    //Sending update as current position because no coupons
  }
);

router.post(
  "/debt/confirm",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  (req, res, next) => {
    //Sending update as current position because no coupons
    redis.get(
      "platform:payments:pending-credits:" + req.body.profile_id,
      function (err, id) {
        if (id === null) {
          send(res, 500, {
            status: "not-found",
          });
        } else {
          log.warn("Checking credit payment product:", id);

          if (id.includes("p-") === true) {
            log.info("Its portal confirmation");
            //Paid option
            let details = _.find(req.body.game.shop.portals, { id: id });
            req.body.id = id;

            let portal_opts = JSON.parse(req.body.counters.portal_opts);
            let option = _.find(portal_opts, { id: id });

            let next_on_map = _.find(req.body.levels, function (l) {
              if (req.body.id === 0) {
              } else {
                return l.COUNTERKEY === option.tile_id;
              }
            });

            Pack.purchaseBoosterAsyncConfirmCredit(
              req,
              res,
              details.service_code,
              function () {
                let move = {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  name: "position",
                  value: option.tile_id,
                };

                Counter.create({ body: move }, function (err, done) {
                  let move_opts = {
                    profile_id: req.body.profile_id,
                    game_id: req.body.game.game_id,
                    name: "portal_opts",
                  };

                  //Removing portal options
                  Counter.remove({ body: move_opts }, function (err, done) {
                    //Check is position was greater then choosed
                    if (
                      parseInt(req.body.counters.position) >
                      parseInt(option.tile_id)
                    ) {
                      //Also reload req.body.counters.steps_stay_here counter to 8 because return back
                      let steps_stay_here = {
                        profile_id: req.body.profile_id,
                        game_id: req.body.game.game_id,
                        name: "steps_stay_here",
                        value: 10,
                      };

                      Counter.create(
                        { body: steps_stay_here },
                        function (err, done) {
                          send(res, 200, {
                            status: "ok",
                            gifts: [],
                          });
                        }
                      );

                      accelera
                        .publishTrigger(req.body.profile_id, "new-round", {
                          game_id: req.body.game.game_id,
                          profile_id: req.body.profile_id,
                          player_id: req.body.player_id,
                        })
                        .then(function () {
                          log.info(
                            "Trigger was published:",
                            "new-round",
                            req.body.profile_id
                          );
                        })
                        .catch((e) => {
                          log.error("Failed to publish trigger:", e);
                        });
                    } else {
                      if (
                        [
                          "p-3",
                          "p-4",
                          "p-5",
                          "p-6",
                          "p-7",
                          "p-7",
                          "p-9",
                          "p-10",
                          "p-11",
                          "p-12",
                        ].includes(option.id) === true
                      ) {
                        //choosed to stay here, decreasing stay here
                        let steps_stay_here = {
                          profile_id: req.body.profile_id,
                          game_id: req.body.game.game_id,
                          name: "steps_stay_here",
                          value: -1,
                        };

                        Counter.modify(
                          { body: steps_stay_here },
                          function (err, done) {
                            send(res, 200, {
                              status: "ok",
                              gifts: [],
                            });
                          }
                        );
                      } else {
                        send(res, 200, {
                          status: "ok",
                          gifts: [],
                        });
                      }
                    }
                  });
                });

                //Update analytics

                let event = {
                  event: "accelera-api",
                  page: "portals",
                  status: "teleported",
                  game_id: req.body.game.game_id,
                  additional: req.body.counters.last_step_uuid,
                  details: (req.body.id + 1).toString(),
                  gifts: [
                    option.tile_id.toString(),
                    next_on_map !== undefined
                      ? next_on_map.COUNTERVALUE
                      : "start",
                    req.body.counters.round.toString(),
                  ],
                  profile_id: req.body.profile_id,
                  player_id:
                    req.body.player_id === undefined
                      ? ""
                      : req.body.player_id.toString(),
                  timestamp: Math.floor(new Date()),
                  date: moment(new Date()).format("YYYY-MM-DD"),
                  time: moment(new Date()).format("HH:mm"),
                  datetime: moment(
                    momentTimezone.tz("Europe/Moscow")._d
                  ).format("YYYY-MM-DD HH:mm:ss"),
                };

                bulk.store(
                  req.body.game.game_id,
                  JSON.stringify(event),
                  function (err) {
                    if (err) {
                      log.error(
                        "Error while storing webhooks messages for Clickhouse bulk:",
                        err
                      );
                    }
                  }
                );
              }
            );
          } else {
            log.info("Its booster confirmation");
            let shop = req.body.game.shop.casts.concat(
              req.body.game.shop.rating
            );
            let booster = _.find(shop, { id: id });
            let purchased = Math.floor(new Date());
            let expired_at = purchased + booster.days * 86400000;

            let rating_booster =
              req.body.counters.rating_booster !== undefined
                ? JSON.parse(req.body.counters.rating_booster).id
                : "";
            let limit_booster =
              req.body.counters.limit_booster !== undefined
                ? JSON.parse(req.body.counters.limit_booster).id
                : "";
            let speed_booster =
              req.body.counters.speed_booster !== undefined
                ? JSON.parse(req.body.counters.speed_booster).id
                : "";

            let prev_booster = "";

            Pack.purchaseBoosterAsyncConfirmCredit(
              req,
              res,
              booster.service_code,
              function () {
                switch (booster.category) {
                  case "limit": {
                    prev_booster = limit_booster;

                    let limit = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "daily_limit",
                      value: 50 + booster.value,
                    };

                    Counter.create({ body: limit }, function (err, done) {
                      let booster_opts = {
                        profile_id: req.body.profile_id,
                        game_id: req.body.game.game_id,
                        name: "limit_booster",
                        value: JSON.stringify({
                          id: booster.id,
                          days: booster.days,
                          value: booster.value,
                          purchased: purchased,
                          expired_at: expired_at,
                        }),
                      };

                      //Removing portal options
                      Counter.create(
                        { body: booster_opts },
                        function (err, done) {
                          send(res, 200, {
                            status: "ok",
                            casts: req.body.game.shop.casts,
                            rating: req.body.game.shop.rating,
                          });
                        }
                      );
                    });

                    break;
                  }

                  case "rating": {
                    prev_booster = rating_booster;

                    let rating = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "multiply_rating",
                      value: booster.value,
                    };

                    Counter.create({ body: rating }, function (err, done) {
                      let booster_opts = {
                        profile_id: req.body.profile_id,
                        game_id: req.body.game.game_id,
                        name: "rating_booster",
                        value: JSON.stringify({
                          id: booster.id,
                          days: booster.days,
                          value: booster.value,
                          steps: booster.steps,
                          purchased: purchased,
                          expired_at: expired_at,
                        }),
                      };

                      //Create booster options
                      Counter.create(
                        { body: booster_opts },
                        function (err, done) {
                          //Reloading steps
                          let steps_opts = {
                            profile_id: req.body.profile_id,
                            game_id: req.body.game.game_id,
                            name: "steps_made",
                            value: 0,
                          };
                          Counter.create(
                            { body: steps_opts },
                            function (err, done) {
                              send(res, 200, {
                                status: "ok",
                                casts: req.body.game.shop.casts,
                                rating: req.body.game.shop.rating,
                              });
                            }
                          );
                        }
                      );
                    });

                    break;
                  }

                  case "speed": {
                    prev_booster = speed_booster;

                    let limit = {
                      profile_id: req.body.profile_id,
                      game_id: req.body.game.game_id,
                      name: "booster_speed",
                      value: booster.multiplier,
                    };

                    Counter.create({ body: limit }, function (err, done) {
                      let booster_opts = {
                        profile_id: req.body.profile_id,
                        game_id: req.body.game.game_id,
                        name: "speed_booster",
                        value: JSON.stringify({
                          id: booster.id,
                          days: booster.days,
                          value: booster.value,
                          purchased: purchased,
                          expired_at: expired_at,
                        }),
                      };

                      //Removing portal options
                      Counter.create(
                        { body: booster_opts },
                        function (err, done) {
                          send(res, 200, {
                            status: "ok",
                            casts: req.body.game.shop.casts,
                            rating: req.body.game.shop.rating,
                          });
                        }
                      );
                    });

                    break;
                  }
                }

                //Update analytics
                let event = {
                  event: "accelera-api",
                  page: "shop",
                  status: "booster-purchased",
                  game_id: req.body.game.game_id,
                  additional: req.body.counters.last_step_uuid,
                  details: booster.id,
                  context: "purchase & activation",
                  gifts: [
                    expired_at.toString(),
                    moment().add(booster.days, "d").format("YYYYMMDD_HHmmss"),
                    booster.category,
                    prev_booster,
                  ],
                  profile_id: req.body.profile_id,
                  player_id:
                    req.body.player_id === undefined
                      ? ""
                      : req.body.player_id.toString(),
                  timestamp: Math.floor(new Date()),
                  date: moment(new Date()).format("YYYY-MM-DD"),
                  time: moment(new Date()).format("HH:mm"),
                  datetime: moment(
                    momentTimezone.tz("Europe/Moscow")._d
                  ).format("YYYY-MM-DD HH:mm:ss"),
                };

                bulk.store(
                  req.body.game.game_id,
                  JSON.stringify(event),
                  function (err) {
                    if (err) {
                      log.error(
                        "Error while storing webhooks messages for Clickhouse bulk:",
                        err
                      );
                    }
                  }
                );
              }
            );
          }
        }
      }
    );
  }
);

module.exports = router;
