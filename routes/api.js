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
const Leaderboard = require("../api/leaderboard");
const _ = require("lodash");
const Probability = require("../services/probabilities");
//External packs method
const Pack = require("../api/packs");

const send = require("@polka/send-type");
const Game = require("../api/games");
const sha = require("../services/sha");
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const bulk = require("../services/bulk");
const Nakama = require("../middleware/nakama");
const rateLimit = require("express-rate-limit");
const crate = require("../services/crateio");
const { json } = require("body-parser");
const redis = require("../services/redis").redisclient;
const jws = require("../services/jws");
const requestIp = require("request-ip");
const axios = require("axios");
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

const authLimiter = rateLimit({
  windowMs: 5 * 60 * 1000, // 5 minutes
  keyGenerator: (request, response) => request.ip,
  max: 10, // Limit each IP to 5 requests per `window` (here, per 5 minutes)
  standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
  legacyHeaders: false, // Disable the `X-RateLimit-*` headers
  message: "Too many requests, please try again later.",
  handler: (request, response, next, options) =>
    send(response, 429, {
      status: "Too many requests, please try again later.",
    }),
});

/* Mobile app authorization with salt for access token */
router.post(
  "/:game_id/auth/token",
  API.mobileChecksum,
  API.getGame,
  API.isWhitelist,
  API.Authorize,
  (req, res, next) => {
    send(res, 200, {
      accessToken: req.body.jwt,
      expires_at: moment(momentTimezone.tz("Europe/Moscow")._d).format(
        "YYYY-MM-DDTHH:mm:ss"
      ),
    });
    //log.warn('Authorized:', req.path, req.body.jwt, req.body.profile_id, req.body.game.game_id, req.body.player_id, req.body.ctn)
  }
);

/* Visits */
router.post(
  "/visit",
  passport.authenticate("api", { session: false }),
  API.getGame,
  API.markVisit,
  (req, res, next) => {
    send(res, 200, {});
  }
);

/* Mobile visit and rules acceptance */
router.post(
  "/:game_id/auth/visit",
  API.getGameByQueryParams,
  API.markVisit,
  Token.Decrypt,
  (req, res, next) => {
    let ip = requestIp.getClientIp(req).toString();
    let up = {
      profile_id: req.body.profile_id,
      ip: ip,
      fingerprint:
        req.body.fingerprint !== undefined
          ? req.body.fingerprint.fingerprint
          : "",
      browser:
        req.body.fingerprint !== undefined ? req.body.fingerprint.browser : "",
      OS: req.body.fingerprint !== undefined ? req.body.fingerprint.OS : "",
      osVersion:
        req.body.fingerprint !== undefined
          ? req.body.fingerprint.osVersion
          : "",
      mobile:
        req.body.fingerprint !== undefined ? req.body.fingerprint.mobile : "",
      device:
        req.body.fingerprint !== undefined ? req.body.fingerprint.device : "",
    };

    Profile.modify({ body: up }, function () {});

    if (req.body.activated === false) {
      send(res, 403, {});
    } else {
      send(res, 200, {});
    }
  }
);

router.put(
  "/:game_id/auth/visit",
  API.getGameByQueryParams,
  Token.Decrypt,
  (req, res, next) => {
    Profile.activate(req.body.decrypted_token, function (err, jwt) {
      send(res, 200, { accessToken: jwt });

      //Update analytics
      let event = {
        event: "accelera-api",
        page: "rules",
        status: "accepted",
        game_id: req.body.game_id,
        profile_id: req.body.profile_id,
        player_id:
          req.body.player_id === undefined ? "" : req.body.player_id.toString(),
        timestamp: Math.floor(new Date()),
        date: moment(new Date()).format("YYYY-MM-DD"),
        time: moment(new Date()).format("HH:mm"),
        datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
          "YYYY-MM-DD HH:mm:ss"
        ),
      };

      bulk.store(req.body.game_id, JSON.stringify(event), function (err) {
        if (err) {
          log.error(
            "Error while storing webhooks messages for Clickhouse bulk:",
            err
          );
        }
      });
    });
  }
);

router.post(
  "/webhooks",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    log.info(
      "Got webhook from the game:",
      req.body.event,
      req.body.profile_id,
      req.body.context
    );
    send(res, 200, {});

    try {
      //Publish trigger
      let context =
        req.body.context !== undefined ? _.cloneDeep(req.body.context) : {};
      context.profile_id = req.body.profile_id;
      context.player_id = req.body.player_id;

      if (
        ["activate-click", "activate-check"].includes(req.body.event) === true
      ) {
        let link = _.find(req.body.game.rewards, { id: context.id });
        context.link = link.link;
      }

      API.publish(
        req.body.profile_id,
        req.body.event,
        isJSONstring(context),
        function () {}
      );

      //Update analytics
      let event = {
        event: "accelera-api",
        page: "webhooks",
        status: "webhook",
        game_id: req.body.game.game_id,
        details: req.body.event.toString(),
        context: JSON.stringify(isJSONstring(context)),
        profile_id: req.body.profile_id,
        player_id:
          req.body.player_id === undefined ? "" : req.body.player_id.toString(),
        timestamp: Math.floor(new Date()),
        date: moment(new Date()).format("YYYY-MM-DD"),
        time: moment(new Date()).format("HH:mm"),
        datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
          "YYYY-MM-DD HH:mm:ss"
        ),
      };

      bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
        if (err) {
          log.error(
            "Error while storing webhooks messages for Clickhouse bulk:",
            err
          );
        }
      });
    } catch (e) {
      log.error(
        "Failed to publish webhook to Accelera Flow:",
        req.body.profile_id,
        req.body.event,
        isJSONstring(req.body.context),
        e
      );
    }

    function isJSONstring(value) {
      if (req.body.context === undefined) return {};
      if (req.body.context === "") return {};

      try {
        JSON.parse(value);
      } catch (e) {
        return value;
      }
      return JSON.parse(value);
    }
  }
);

/* Authorization */
router.post(
  "/auth",
  passport.authenticate("api", { session: false }),
  API.getGame,
  API.isBlockedCTN,
  API.isWhitelist,
  birthdayLimiter,
  (req, res, next) => {
    let num = (Math.floor(Math.random() * 1000000) + 1000000)
      .toString()
      .substring(1); //6 digits
    let salt = "8242m4aaafs524288da-sd88242m4d9a8242m49r-sdU7HTuKab78G-~ns~##";
    let target = "+" + req.body.ctn;
    if (target.includes("+380") === false) {
      SMS.send(
        {
          profile_id: req.body.ctn,
          player_id: req.body.ctn,
          game_id: req.body.game.game_id,
          message:
            "Проверочный код в игре «" + req.body.game.title + "»: " + num,
          target: "+" + req.body.ctn,
        },
        function (err, ok) {
          if (err) {
            log.error("Failed to send SMS:", req.body.ctn, err);
            send(res, 500, {});
          } else {
            //Publish trigger
            API.publish(
              req.body.ctn,
              "sms_sent",
              {
                profile_id: req.body.ctn,
                player_id: req.body.ctn,
                game_id: req.body.game.game_id,
                message: "Проверочный код: " + num,
                target: "+" + req.body.ctn,
                checksum: sha.encrypt(num.toString() + salt),
              },
              function () {}
            );

            log.info("Auth SMS was sent:", req.body.ctn, num);
            send(res, 200, { checksum: sha.encrypt(num.toString() + salt) });
          }
        }
      );

      //Check if game is XMAS
      /*       if (req.body.game.game_id === 'xmas') {
            crate.getRewardsByCTN(req.body.ctn.substring(1,11), function (err, metkauser){
                log.info('Got user from crate.io:', metkauser);
                if (metkauser !== undefined && !err) {
                    //User is in list
                    SMS.send({
                        profile_id: req.body.ctn,
                        player_id: req.body.ctn,
                        game_id: req.body.game.game_id,
                        message: "Проверочный код в игре «"+req.body.game.title+"»: " + num,
                        target: '+'+req.body.ctn
                    }, function (err, ok){
                        if (err) {
                            log.error('Failed to send SMS:', req.body.ctn, err);
                            send(res, 500, {});

                        } else {
                            //Publish trigger
                            API.publish(req.body.ctn, 'sms_sent', {
                                profile_id: req.body.ctn,
                                player_id: req.body.ctn,
                                game_id: req.body.game.game_id,
                                message: "Проверочный код: " + num,
                                target: '+'+req.body.ctn,
                                checksum: sha.encrypt(num.toString() + salt)
                            }, function (){})

                            send(res, 200, {"checksum" : sha.encrypt(num.toString() + salt)});
                        }
                    })

                } else {
                    send(res, 404);
                }
            })
        } else {
            SMS.send({
                profile_id: req.body.ctn,
                player_id: req.body.ctn,
                game_id: req.body.game.game_id,
                message: "Проверочный код в игре «"+req.body.game.title+"»: " + num,
                target: '+'+req.body.ctn
            }, function (err, ok){
                if (err) {
                    log.error('Failed to send SMS:', req.body.ctn, err);
                    send(res, 500, {});

                } else {
                    //Publish trigger
                    API.publish(req.body.ctn, 'sms_sent', {
                        profile_id: req.body.ctn,
                        player_id: req.body.ctn,
                        game_id: req.body.game.game_id,
                        message: "Проверочный код: " + num,
                        target: '+'+req.body.ctn,
                        checksum: sha.encrypt(num.toString() + salt)
                    }, function (){})

                    send(res, 200, {"checksum" : sha.encrypt(num.toString() + salt)});
                }
            })
        }*/
    } else {
      send(res, 200);
    }
  }
);

router.post(
  "/confirm",
  passport.authenticate("api", { session: false }),
  API.getGame,
  API.isBlockedCTN,
  API.checkConfirmationCode,
  API.Authorize,
  (req, res, next) => {
    log.debug(
      "SMS code is confirmed, authorized",
      req.body.ctn,
      req.body.connection_ip
    );
    send(res, 200, { token: req.body.jwt });

    //Checkup for invite, bring a friend
    if (req.body.invite !== undefined && req.body.invite !== "") {
      log.info("Invited by friend:", req.body.invite);

      API.publish(
        req.body.invite,
        "invite",
        {
          game_id: req.body.game.game_id,
          profile_id: req.body.profile_id,
          invite_for: req.body.profile_id,
          invited_by: req.body.invite,
        },
        function () {}
      );
    }

    //GORKY PARK
    if (req.body.gorky !== undefined && req.body.gorky !== "") {
      log.info("QR code is found:", req.body.gorky);

      API.publish(
        req.body.profile_id,
        "qr_trigger",
        {
          game_id: req.body.game.game_id,
          profile_id: req.body.profile_id,
          qr_name: req.body.gorky,
        },
        function () {}
      );
    }
  }
);

/* Games */
router.post(
  "/games",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  (req, res, next) => {
    if (req.body.game.private.disabled_methods !== undefined) {
      if (
        req.body.game.private.disabled_methods.includes("api/games") === true
      ) {
        return send(res, 403, { status: "forbidden" });
      } else {
        Counter.findbyprofile(req, function (err, counters) {
          if (err) return send(res, 500, { status: "failed" });

          req.body.game.timestamp = Math.floor(new Date());

          //Adding free try parameters
          Pack.checkFreePack(req, counters, function (err) {
            //Reformat leaderboard daily/weekly/monthly data
            let daily_reformatted = [];
            for (let i in req.body.game.leaderboard.gifts.daily) {
              if (
                typeof req.body.game.leaderboard.gifts.daily[i].place ===
                "string"
              ) {
                reformat(
                  req.body.game.leaderboard.gifts.daily[i].place.split("-")[0],
                  req.body.game.leaderboard.gifts.daily[i].place.split("-")[1],
                  req.body.game.leaderboard.gifts.daily[i]
                );
              } else {
                daily_reformatted.push(
                  req.body.game.leaderboard.gifts.daily[i]
                );
              }
            }

            //Reformat leaderboard daily/weekly/monthly data
            let weekly_reformatted = [];
            for (let i in req.body.game.leaderboard.gifts.weekly) {
              if (
                typeof req.body.game.leaderboard.gifts.weekly[i].place ===
                "string"
              ) {
                reformat_weekly(
                  req.body.game.leaderboard.gifts.weekly[i].place.split("-")[0],
                  req.body.game.leaderboard.gifts.weekly[i].place.split("-")[1],
                  req.body.game.leaderboard.gifts.weekly[i]
                );
              } else {
                weekly_reformatted.push(
                  req.body.game.leaderboard.gifts.weekly[i]
                );
              }
            }

            //Reformat leaderboard daily/weekly/monthly data
            let monthly_reformatted = [];
            for (let i in req.body.game.leaderboard.gifts.monthly) {
              if (
                typeof req.body.game.leaderboard.gifts.monthly[i].place ===
                "string"
              ) {
                reformat_monthly(
                  req.body.game.leaderboard.gifts.monthly[i].place.split(
                    "-"
                  )[0],
                  req.body.game.leaderboard.gifts.monthly[i].place.split(
                    "-"
                  )[1],
                  req.body.game.leaderboard.gifts.monthly[i]
                );
              } else {
                monthly_reformatted.push(
                  req.body.game.leaderboard.gifts.monthly[i]
                );
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
            res.end(JSON.stringify(req.body.game));
          });
        });

        if (req.body.fingerprint !== undefined) {
          let fp = {
            profile_id: req.body.profile_id,
            fingerprint: req.body.fingerprint.fingerprint,
            browser: req.body.fingerprint.browser,
            OS: req.body.fingerprint.OS,
            osVersion: req.body.fingerprint.osVersion,
          };
          API.publish(req.body.profile_id, "fingerprint", fp, function () {});
        }
      }
    } else {
      Counter.findbyprofile(req, function (err, counters) {
        if (err) return send(res, 500, { status: "failed" });

        req.body.game.timestamp = Math.floor(new Date());

        //Adding free try parameters
        Pack.checkFreePack(req, counters, function (err) {
          //Reformat leaderboard daily/weekly/monthly data
          let daily_reformatted = [];
          for (let i in req.body.game.leaderboard.gifts.daily) {
            if (
              typeof req.body.game.leaderboard.gifts.daily[i].place === "string"
            ) {
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
            if (
              typeof req.body.game.leaderboard.gifts.weekly[i].place ===
              "string"
            ) {
              reformat_weekly(
                req.body.game.leaderboard.gifts.weekly[i].place.split("-")[0],
                req.body.game.leaderboard.gifts.weekly[i].place.split("-")[1],
                req.body.game.leaderboard.gifts.weekly[i]
              );
            } else {
              weekly_reformatted.push(
                req.body.game.leaderboard.gifts.weekly[i]
              );
            }
          }

          //Reformat leaderboard daily/weekly/monthly data
          let monthly_reformatted = [];
          for (let i in req.body.game.leaderboard.gifts.monthly) {
            if (
              typeof req.body.game.leaderboard.gifts.monthly[i].place ===
              "string"
            ) {
              reformat_monthly(
                req.body.game.leaderboard.gifts.monthly[i].place.split("-")[0],
                req.body.game.leaderboard.gifts.monthly[i].place.split("-")[1],
                req.body.game.leaderboard.gifts.monthly[i]
              );
            } else {
              monthly_reformatted.push(
                req.body.game.leaderboard.gifts.monthly[i]
              );
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
          res.end(JSON.stringify(req.body.game));
        });
      });

      if (req.body.fingerprint !== undefined) {
        let fp = {
          profile_id: req.body.profile_id,
          fingerprint: req.body.fingerprint.fingerprint,
          browser: req.body.fingerprint.browser,
          OS: req.body.fingerprint.OS,
          osVersion: req.body.fingerprint.osVersion,
        };
        API.publish(req.body.profile_id, "fingerprint", fp, function () {});
      }
    }
  }
);

/* Counters */
router.post(
  "/counters",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Counter.findbyprofile(req, function (err, counter) {
      if (err) return send(res, 500, { status: "failed" });

      let counters = {};
      let levels_ = {};

      //Extract levels
      for (let i in Object.keys(counter)) {
        if (
          Object.keys(counter)[i].includes("level") === true &&
          Object.keys(counter)[i] !== "last_level"
        ) {
          let level = Object.keys(counter)[i].split("_");
          levels_[level[1]] = counter[Object.keys(counter)[i]];
        } else {
          counters[Object.keys(counter)[i]] = counter[Object.keys(counter)[i]];
        }
      }
      counters.levels = levels_;

      res.end(JSON.stringify(counters));
    });
  }
);

/* Achievements (Collections) */
router.post(
  "/achievements",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Achievement.findbyprofile(req, function (err, achievement) {
      if (err) return send(res, 500, { status: "failed" });
      res.end(JSON.stringify(achievement));
    });
  }
);

/* Rewards (History of the gifts) */
router.post(
  "/rewards",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Rewards.findbyprofile(req, function (err, rewards) {
      if (err) return send(res, 500, { status: "failed" });

      let sorted = [];
      for (let i in rewards) {
        let gamerewards = _.find(req.body.game.rewards, { id: rewards[i].id });
        rewards[i].description =
          gamerewards !== undefined
            ? gamerewards.full_description
            : rewards[i].description;
        rewards[i].name =
          gamerewards !== undefined
            ? gamerewards.short_description
            : rewards[i].name;
        //If gift not found - do not show promocode
        //rewards[i].promocode = (gamerewards !== undefined) ? gamerewards.promocode : 'NA';
        rewards[i].reward_id = i;
        rewards[i].link =
          gamerewards !== undefined
            ? decodeHTMLEntities(gamerewards.link)
            : rewards[i].link;
        sorted.push(rewards[i]);
      }

      res.end(JSON.stringify(_.sortBy(sorted, "timestamp").reverse()));
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

/* Dialogs */
router.post(
  "/dialogs",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Dialog.findbyprofile(req, function (err, dialog) {
      if (err) return send(res, 500, { status: "failed" });
      res.end(JSON.stringify(dialog));
    });
  }
);

/* Tasks */
router.post(
  "/tasks",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Task.findbyprofile(req, function (err, task) {
      if (err) return send(res, 500, { status: "failed" });
      res.end(JSON.stringify(task));
    });
  }
);

/* Items */
router.post(
  "/items",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Items.findbyprofile(req, function (err, items) {
      if (err) return send(res, 500, { status: "failed" });
      res.end(JSON.stringify(items));
    });
  }
);

/* Increments */
router.post(
  "/increments",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Increment.findbyprofile(req, function (err, increments) {
      if (err) return send(res, 500, { status: "failed" });
      res.end(JSON.stringify(increments));
    });
  }
);

/* Profiles */
router.post(
  "/profiles",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    Profile.get(req.body.profile_id, function (err, profile) {
      if (err) return send(res, 500, { status: "failed" });
      res.end(JSON.stringify(profile));
    });
  }
);

/* Leaderboard with daily rating points update */
router.post(
  "/leaderboard",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.reloadDailyLeaderboard,
  (req, res, next) => {
    Leaderboard.get(req, function (err, leaderboard) {
      if (err) return send(res, 500, { status: "failed" });

      //Adding
      res.end(JSON.stringify(leaderboard));
    });
  }
);

router.post(
  "/leaderboard/purchase",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  (req, res, next) => {
    Pack.purchaseLeaderboard(req, res, function (err) {
      if (err) return send(res, 500, { status: "failed" });

      Leaderboard.get(req, function (err, leaderboard) {
        if (err) return send(res, 500, { status: "failed" });

        send(res, 200, { status: "purchased", leaderboard: leaderboard });
      });

      //Update analytics
      let event = {
        event: "accelera-api",
        page: "packs",
        status: "purchased",
        game_id: req.body.game.game_id,
        details: req.body.pack.toString(),
        gifts: [
          "0",
          "0",
          req.body.product.pointsrate.toString(),
          req.body.productId.toString(),
        ], //+tries, current balance, expense in rub
        profile_id: req.body.profile_id,
        player_id:
          req.body.player_id === undefined ? "" : req.body.player_id.toString(),
        timestamp: Math.floor(new Date()),
        date: moment(new Date()).format("YYYY-MM-DD"),
        time: moment(new Date()).format("HH:mm"),
        datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
          "YYYY-MM-DD HH:mm:ss"
        ),
      };

      bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
        if (err) {
          log.error(
            "Error while storing webhooks messages for Clickhouse bulk:",
            err
          );
        }
      });
    });
  }
);

/* External / Packs */
router.post(
  "/packs/free/get",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  (req, res, next) => {
    Counter.findbyprofile(req, function (err, counters) {
      if (err) return send(res, 500, { status: "failed" });

      //Adding free try parameters
      Pack.getFreePack(req, counters, function (err, updates) {
        //Reply
        if (err) return send(res, 500, { status: "failed" });
        res.end(JSON.stringify(updates));

        //Publish trigger
        API.publish(
          req.body.profile_id,
          "freepack",
          req.body.decrypted_token,
          function () {}
        );

        //Update analytics
        let event = {
          event: "accelera-api",
          page: "got-free-pack",
          status: "succeed",
          game_id: req.body.game.game_id,
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
    });
  }
);

router.post(
  "/packs/purchase",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  (req, res, next) => {
    log.info("Purchasing a pack:", req.body);

    Pack.purchasePack(req, res, function (err, updates) {
      if (err) return send(res, 500, { status: "FAILED" });
      send(res, 200, { status: "purchased", counters: updates });

      //Publish trigger & pack
      req.body.decrypted_token.pack = req.body.pack;
      API.publish(
        req.body.profile_id,
        "purchase",
        req.body.decrypted_token,
        function () {}
      );

      //Update analytics
      let event = {
        event: "accelera-api",
        page: "packs",
        status: "purchased",
        game_id: req.body.game.game_id,
        details: req.body.pack.toString(),
        gifts: [
          req.body.product.rate.toString(),
          updates.tries.toString(),
          req.body.product.pointsrate.toString(),
          req.body.productId.toString(),
        ], //+tries, current balance, expense in rub
        profile_id: req.body.profile_id,
        player_id:
          req.body.player_id === undefined ? "" : req.body.player_id.toString(),
        timestamp: Math.floor(new Date()),
        date: moment(new Date()).format("YYYY-MM-DD"),
        time: moment(new Date()).format("HH:mm"),
        datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
          "YYYY-MM-DD HH:mm:ss"
        ),
      };

      bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
        if (err) {
          log.error(
            "Error while storing webhooks messages for Clickhouse bulk:",
            err
          );
        }
      });
    });
  }
);

router.post(
  "/partners/get",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  (req, res, next) => {
    log.info("Getting partners gift:", req.body);

    Rewards.getMatchPartners(req, res, function (err, partner) {
      send(res, 200, { status: "ok", partner: partner });
    });
  }
);

//Открытие главного приза в 30 лет
router.post(
  "/treasure/open",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  API.Counters,
  (req, res, next) => {
    log.info("Getting partners gift as a treasure", req.body.profile_id);

    let attempt = {
      profile_id: req.body.profile_id,
      game_id: req.body.game.game_id,
      name: req.body.game.private.sessions.attempt_counter,
      value: 1,
    };

    if (req.body.game.private.sessions.attempt_counter !== undefined) {
      let attempt_val =
        req.body.counters[req.body.game.private.sessions.attempt_counter];
      let stars_val = req.body.counters[req.body.game.private.sessions.counter];
      if (parseInt(attempt_val) < parseInt(stars_val)) {
        Counter.modify({ body: attempt }, function (err, updates) {
          //Update analytics
          let updated = updates[req.body.game.private.sessions.attempt_counter];

          //Invoke formula to get gift

          Probability.getItemByProbability(req, function (err, rewards, prob) {
            if (err)
              return send(res, 200, {
                status: "ok",
                tries: updated,
                reward: {},
              });

            //Check if something
            if (rewards.id === "b-00") {
              send(res, 200, { status: "ok", tries: updated, reward: {} });
              API.publish(
                req.body.profile_id,
                "no-super-prize",
                {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                },
                function () {}
              );
            } else {
              let gift = _.find(req.body.game.rewards, { id: rewards.id });

              //Sending event
              let supergift = _.cloneDeep(gift);
              supergift["profile_id"] = req.body.profile_id;

              //Getting from coupons
              Profile.getBirthdayCoupon(
                supergift,
                function (err, personalized) {
                  if (!err) {
                    send(res, 200, {
                      status: "ok",
                      tries: updated,
                      reward: supergift,
                    });
                    API.publish(
                      req.body.profile_id,
                      "super-prize",
                      personalized,
                      function () {}
                    );

                    let event = {
                      event: "accelera-api",
                      page: "open-treasure",
                      status: "succeed",
                      game_id: req.body.game.game_id,
                      context: JSON.stringify(req.body.counters),
                      gifts: [
                        req.body.game.private.sessions.attempt_counter,
                        "1",
                        updated.toString(),
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
                  } else {
                    send(res, 500, { status: "failed" });

                    let event = {
                      event: "accelera-api",
                      page: "open-treasure",
                      status: "failed",
                      game_id: req.body.game.game_id,
                      context: JSON.stringify(req.body.counters),
                      gifts: [
                        req.body.game.private.sessions.attempt_counter,
                        "1",
                        updated.toString(),
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
                }
              );
            }
          });
        });
      } else {
        send(res, 500, {
          status: "not_enough_balance",
          tries: parseInt(attempt_val),
          reward: {},
        });
      }
    } else {
      send(res, 500, { status: "not_enough_balance", reward: {} });
    }
  }
);

//SUBSCRIPTIONS
router.post(
  "/subscriptions/get",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  API.Counters,
  (req, res, next) => {
    log.info("Getting subscriptions:", req.body);

    send(res, 200, {
      status: "ok",
      subscriptions: [
        {
          id: 238,
          name: "Подписка на ускорение таймера ежедневных попыток x3",
          description:
            "Ускорение времени восстановления ежедневных попыток в 3 раза",
          price: 3,
          statusInfo: null,
        },
        {
          id: 241,
          name: "Подписка на увеличение ежедневных попыток x12",
          description:
            "Увеличение количества получаемых ежедневных попыток в 12 раз",
          price: 7.5,
          statusInfo: null,
        },
        {
          id: 239,
          name: "Подписка на ускорение таймера ежедневных попыток x6",
          description:
            "Ускорение времени восстановления ежедневных попыток в 6 раз",
          price: 5,
          statusInfo: null,
        },
        {
          id: 242,
          name: "Подписка на увеличение и ускорение получения ежедневных попыток x4",
          description:
            "Увеличение количества и ускорение получаемых ежедневных попыток в 4 раза",
          price: 7,
          statusInfo: null,
        },
        {
          id: 240,
          name: "Подписка на увеличение ежедневных попыток x4",
          description:
            "Увеличение количества получаемых ежедневных попыток в 4 раза",
          price: 4.75,
          statusInfo: null,
        },
      ],
    });
  }
);

router.post(
  "/subscriptions/activate",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  Pack.purchaseSubscription,
  (req, res, next) => {
    log.info("Activating subscriptions:", req.body);

    let fp = {
      profile_id: req.body.profile_id,
      pack: req.body.pack,
    };
    API.publish(
      req.body.profile_id,
      "activate_subscription",
      fp,
      function () {}
    );

    send(res, 200, { status: "ok" });
  }
);

router.post(
  "/subscriptions/deactivate",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  Pack.deactivateSubscription,
  (req, res, next) => {
    log.info("Deactivating subscriptions:", req.body);

    let fp = {
      profile_id: req.body.profile_id,
      pack: req.body.pack,
    };
    API.publish(
      req.body.profile_id,
      "deactivate_subscription",
      fp,
      function () {}
    );

    send(res, 200, { status: "ok" });
  }
);

/* Game sessions */
/*
router.post('/sessions/get', passport.authenticate('api', { session: false}), birthdayLimiter, API.getGame, Token.Decrypt, API.isBlocked, API.Counters, API.createSession, API.getCrateLevelsbyMetka, (req, res, next) => {
    log.debug('New game session is created:', req.body.profile_id, req.body.game_id, req.body.session);
    send(res, 200, {"session" : req.body.session});


    //Sending session result event to a flow
    API.publish(req.body.profile_id, 'session_started', {
        "profile_id" : req.body.profile_id,
        "game_id" : req.body.game.game_id,
        "player_id" : req.body.profile_id,
        "session" : req.body.session,
        "level" : req.body.level
    }, function (){})
});
*/

// Proceed game result
router.post(
  "/sessions/proceed",
  passport.authenticate("api", { session: false }),
  birthdayLimiter,
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  API.Counters,
  API.proceedSession,
  API.getCrateLevelsbyMetka,
  (req, res, next) => {
    log.debug(
      "Game session is proceeded:",
      req.body.profile_id,
      req.body.game_id,
      req.body.session,
      req.body.result
    );
    let current =
      req.body.counters["level_" + req.body.session_data.level] !== undefined
        ? req.body.counters["level_" + req.body.session_data.level]
        : 0;

    if (req.body.session_data.level !== undefined) {
      Rewards.findbyprofile(req, function (err, issued) {
        if (err) {
          send(res, 200, { session: req.body.session, reward: {} });

          //Sending session result event to a flow
          API.publish(
            req.body.profile_id,
            "session_finished",
            {
              profile_id: req.body.profile_id,
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              session: req.body.session,
              level: req.body.session_data.level,
              min: req.body.session_data.min,
              max: req.body.session_data.max,
              current:
                req.body.counters["level_" + req.body.session_data.level] ===
                undefined
                  ? 0
                  : req.body.counters["level_" + req.body.session_data.level],
              result: req.body.result,
              counter: req.body.game.private.sessions.counter,
              rewarded: "false",
              reward: {},
            },
            function () {}
          );

          //Storing to clickhouse
          let event = {
            event: "accelera-api",
            page: "sessions",
            status: "stored",
            game_id: req.body.game.game_id,
            player_id: req.body.player_id,
            profile_id: req.body.profile_id,
            context: JSON.stringify({}),
            additional: JSON.stringify({
              level: req.body.session_data.level,
              result: req.body.result,
            }),
            details: req.body.session,
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
          let issued_partners = Object.keys(issued);
          let current_level = "game_place_" + req.body.session_data.level;
          let reward_on_level = req.body.levels[current_level];
          log.info(
            "Searhing for level reward:",
            issued_partners,
            reward_on_level
          );
          if (issued_partners.includes(reward_on_level) === false) {
            let reward = _.find(req.body.game.rewards, { id: reward_on_level });

            //Getting from coupons
            Profile.getBirthdayCoupon(reward, function (err, personalized) {
              personalized.promocode = personalized.coupon;
              let updatedlink = decodeHTMLEntities(personalized.link);
              personalized.link = updatedlink;
              send(res, 200, {
                session: req.body.session,
                reward: personalized,
              });

              //Sending session result event to a flow
              API.publish(
                req.body.profile_id,
                "session_finished",
                {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  player_id: req.body.player_id,
                  session: req.body.session,
                  min: req.body.session_data.min,
                  max: req.body.session_data.max,
                  current:
                    req.body.counters[
                      "level_" + req.body.session_data.level
                    ] === undefined
                      ? 0
                      : req.body.counters[
                          "level_" + req.body.session_data.level
                        ],
                  result: req.body.result,
                  counter: req.body.game.private.sessions.counter,
                  level: req.body.session_data.level,
                  rewarded: "true",
                  sms_type: reward.sms_type,
                  sms_1: reward.sms_1,
                  sms_2: reward.sms_2,
                  reward: personalized,
                },
                function () {}
              );

              //Storing to clickhouse
              let event = {
                event: "accelera-api",
                page: "sessions",
                status: "stored",
                game_id: req.body.game.game_id,
                player_id: req.body.player_id,
                profile_id: req.body.profile_id,
                context: JSON.stringify(reward),
                additional: JSON.stringify({
                  level: req.body.session_data.level,
                  result: req.body.result,
                }),
                details: req.body.session,
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
            let existing_reward = issued[reward_on_level];
            let updatedlink = decodeHTMLEntities(existing_reward.link);
            existing_reward.link = updatedlink;

            send(res, 200, {
              session: req.body.session,
              reward: existing_reward,
            });

            //Sending session result event to a flow
            API.publish(
              req.body.profile_id,
              "session_finished",
              {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                player_id: req.body.player_id,
                session: req.body.session,
                min: req.body.session_data.min,
                max: req.body.session_data.max,
                current:
                  req.body.counters["level_" + req.body.session_data.level] ===
                  undefined
                    ? 0
                    : req.body.counters["level_" + req.body.session_data.level],
                result: req.body.result,
                counter: req.body.game.private.sessions.counter,
                level: req.body.session_data.level,
                rewarded: "false",
                reward: {},
              },
              function () {}
            );

            //Storing to clickhouse
            let event = {
              event: "accelera-api",
              page: "sessions",
              status: "stored",
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              profile_id: req.body.profile_id,
              context: JSON.stringify({}),
              additional: JSON.stringify({
                level: req.body.session_data.level,
                result: req.body.result,
              }),
              details: req.body.session,
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
      });
    } else {
      log.error("No level in session data!");
      send(res, 200, { session: req.body.session, reward: {} });
    }

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
  "/sessions/activate",
  passport.authenticate("api", { session: false }),
  birthdayLimiter,
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  API.Counters,
  API.getCrateLevelsbyMetka,
  (req, res, next) => {
    log.debug(
      "Game gift is going to be activated:",
      req.body.profile_id,
      req.body.game_id,
      req.body.level
    );
    if (req.body.level !== undefined) {
      Rewards.findbyprofile(req, function (err, issued) {
        if (err) {
          send(res, 500, { status: "failed", reward: {} });

          //Storing to clickhouse
          let event = {
            event: "accelera-api",
            page: "rewards",
            status: "reward-activation-failed",
            game_id: req.body.game.game_id,
            player_id: req.body.player_id,
            profile_id: req.body.profile_id,
            context: JSON.stringify({}),
            additional: JSON.stringify({ level: req.body.level }),
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
          let issued_partners = Object.keys(issued);
          let current_level = "game_place_" + req.body.level;
          let reward_on_level = req.body.levels[current_level];

          log.info(
            "Searhing for level reward:",
            issued_partners,
            reward_on_level
          );
          if (issued_partners.includes(reward_on_level) === false) {
            let reward = _.find(req.body.game.rewards, { id: reward_on_level });

            if (reward.status !== "active")
              return send(res, 500, { status: "failed", reward: {} });
            //Getting from coupons
            Profile.getBirthdayCoupon(reward, function (err, personalized) {
              personalized.promocode = personalized.coupon;
              let updatedlink = decodeHTMLEntities(personalized.link);
              personalized.link = updatedlink;

              send(res, 200, { status: "ok", reward: personalized });

              //Sending session result event to a flow
              API.publish(
                req.body.profile_id,
                "session_finished",
                {
                  profile_id: req.body.profile_id,
                  game_id: req.body.game.game_id,
                  player_id: req.body.player_id,
                  session: "",
                  min: 0,
                  max: 0,
                  current:
                    req.body.counters["level_" + req.body.level] === undefined
                      ? 0
                      : req.body.counters["level_" + req.body.level],
                  result: 0,
                  counter: req.body.game.private.sessions.counter,
                  level: req.body.level,
                  rewarded: "true",
                  sms_type: reward.sms_type,
                  sms_1: reward.sms_1,
                  sms_2: reward.sms_2,
                  reward: personalized,
                },
                function () {}
              );

              //Storing to clickhouse
              let event = {
                event: "accelera-api",
                page: "rewards",
                status: "reward-activation-passed",
                game_id: req.body.game.game_id,
                player_id: req.body.player_id,
                profile_id: req.body.profile_id,
                context: JSON.stringify(reward),
                additional: JSON.stringify({
                  level: req.body.level,
                  reward: personalized.id,
                }),
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
            let existing_reward = issued[reward_on_level];
            let updatedlink = decodeHTMLEntities(existing_reward.link);
            existing_reward.link = updatedlink;

            send(res, 200, { status: "ok", reward: existing_reward });

            //Storing to clickhouse
            let event = {
              event: "accelera-api",
              page: "rewards",
              status: "reward-activation-restored",
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              profile_id: req.body.profile_id,
              context: JSON.stringify({}),
              additional: JSON.stringify({
                level: req.body.level,
                reward: existing_reward.id,
              }),
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
      });
    } else {
      log.error("No level in session data!");
      send(res, 500, { status: "failed", reward: {} });
    }

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

//Services activation from landing (no token)
router.post(
  "/services/request",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Pack.sendServiceCode,
  (req, res, next) => {
    send(res, 200, {});
  }
);

router.post(
  "/services/confirm",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Pack.confirmServiceCode,
  (req, res, next) => {
    send(res, 200, {});
  }
);

//Services activation for partners gifts (activation_type = ingame_partner)
router.post(
  "/services_partners/request",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  Pack.sendPartnerActivationRequest,
  (req, res, next) => {
    send(res, 200, {});
  }
);

router.post(
  "/services_partners/confirm",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  API.checkDecision,
  Pack.sendPartnerActivationConfirm,
  (req, res, next) => {
    send(res, 200, {
      status:
        "Запрос на подключение услуги принят, ждите СМС со статусом подключения!",
      modal: "end",
    });
  }
);

//Services activation from games (with token)
router.post(
  "/services/requestByToken",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  Pack.sendServiceCode,
  (req, res, next) => {
    send(res, 200, {});
  }
);

router.post(
  "/services/confirmByToken",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.isBlocked,
  Pack.confirmServiceCode,
  (req, res, next) => {
    send(res, 200, {});
  }
);

//Just for dev: test method for taxi TODO: delete it later
router.post(
  "/taxi/auth",
  passport.authenticate("api", { session: false }),
  (req, res, next) => {
    //log.warn('Got Taxi auth request:', req.body);
    let jwt = jws.encrypt({
      game_id: "taxi",
      player_id: req.body.id,
      profile_id: req.body.id,
      timestamp: Math.floor(new Date()),
    });

    send(res, 200, {
      url: "https://city.cubesolutions.ru/taxi/index.html?token=" + jwt,
    });
  }
);

router.post(
  "/taxi/settings",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    redis.hget(
      "platform:game:taxi:level",
      req.body.decrypted_token.profile_id,
      function (err, level) {
        send(res, 200, {
          status: "ok",
          level: level === null ? 0 : parseInt(level),
        });
      }
    );
  }
);

router.post(
  "/taxi/step",
  passport.authenticate("api", { session: false }),
  (req, res, next) => {
    log.warn("Got Taxi step request:", req.body);
    send(res, 200, {});
  }
);

//Проверка нужно завершить игру или нет
router.post(
  "/taxi/checkout",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  (req, res, next) => {
    //log.warn('Got Taxi checkout request:', req.body)
    redis.hget(
      "platform:game:taxi:stop",
      req.body.decrypted_token.profile_id,
      function (err, result) {
        if (result !== null) {
          send(res, 200, { result: "stop" });
          redis.hdel(
            "platform:game:taxi:stop",
            req.body.decrypted_token.profile_id,
            function () {}
          );
        } else {
          send(res, 200, { result: "ok" });
        }
      }
    );
  }
);

router.post(
  "/taxi/webhooks",
  passport.authenticate("api", { session: false }),
  (req, res, next) => {
    //log.warn('Got Taxi webhook request:', req.body)
    //Its stop event
    redis.hset("platform:game:taxi:stop", req.body.id, "stop", function (err) {
      send(res, 200, {});
    });
  }
);

router.post(
  "/taxi/game",
  passport.authenticate("api", { session: false }),
  (req, res, next) => {
    //log.warn('Got Taxi game request:', req.body)
    let token = jws.decrypt(req.body.token);
    redis.hset(
      "platform:game:taxi:level",
      token.profile_id,
      req.body.context.level,
      function (err, level) {
        send(res, 200, { status: "ok" });
      }
    );
  }
);

router.post("/activations/callback", (req, res, next) => {
  log.warn("Got activation callback:", req.body);

  //Getting profile by CTN
  Profile.findbyuser(
    {
      body: {
        id: req.body.phone.substring(1, 12),
        system: "xmas2023",
      },
    },
    function (err, profile) {
      if (req.body.actionType === "tariff_connection") {
        //Its Up activation
        switch (req.body.status) {
          case "success": {
            log.warn(
              "Publish success UP tariff activation",
              profile.profile_id,
              profile.id
            );
            API.publish(
              profile.profile_id,
              "up-success",
              {
                game_id: "xmas2023",
                profile_id: profile.profile_id,
                player_id: profile.id,
              },
              function () {}
            );
            break;
          }

          case "error": {
            if (req.body.error.info === "RULE_CODE_BALANCE") {
              log.warn(
                "Publish failed UP tariff activation",
                profile.profile_id,
                profile.id,
                req.body.error
              );
              API.publish(
                profile.profile_id,
                "up-balance",
                {
                  game_id: "xmas2023",
                  profile_id: profile.profile_id,
                  player_id: profile.id,
                },
                function () {}
              );
            } else {
              log.warn(
                "Publish failed UP tariff activation",
                profile.profile_id,
                profile.id,
                req.body.error
              );
              API.publish(
                profile.profile_id,
                "up-failed",
                {
                  game_id: "xmas2023",
                  profile_id: profile.profile_id,
                  player_id: profile.id,
                },
                function () {}
              );
            }
            break;
          }
        }
      }
    }
  );

  send(res, 200, { status: "ok" });

  //Also transferring to DEV
  /*axios({
        method: 'POST',
        url: 'https://g1-dev.accelera.ai/api/activations_dev/callback',
        headers: {
            "Content-Type": "application/json",
        },
        data: req.body,
        timeout: 30000
    }).then(response => {
        log.debug('Transferred activation request:', response.status, req.body);
    }).catch(err => {
        log.error('Transfer activation request was failed', req.body, err);
    });*/
});

router.post("/activations_dev/callback", (req, res, next) => {
  log.warn("Got activation callback:", req.body);

  //Getting profile by CTN
  Profile.findbyuser(
    {
      body: {
        id: req.body.phone.substring(1, 12),
        system: "xmas2023",
      },
    },
    function (err, profile) {
      if (req.body.actionType === "tariff_connection") {
        //Its Up activation
        switch (req.body.status) {
          case "success": {
            log.warn(
              "Publish success UP tariff activation",
              profile.profile_id,
              profile.id
            );
            API.publish(
              profile.profile_id,
              "up-success",
              {
                game_id: "xmas2023",
                profile_id: profile.profile_id,
                player_id: profile.id,
                date_up: "14.12.2023",
              },
              function () {}
            );
            break;
          }

          case "error": {
            if (req.body.error.info === "RULE_CODE_BALANCE") {
              log.warn(
                "Publish failed UP tariff activation",
                profile.profile_id,
                profile.id,
                req.body.error
              );
              API.publish(
                profile.profile_id,
                "up-balance",
                {
                  game_id: "xmas2023",
                  profile_id: profile.profile_id,
                  player_id: profile.id,
                  date_up: "14.12.2023",
                },
                function () {}
              );
            } else {
              log.warn(
                "Publish failed UP tariff activation",
                profile.profile_id,
                profile.id,
                req.body.error
              );
              API.publish(
                profile.profile_id,
                "up-failed",
                {
                  game_id: "xmas2023",
                  profile_id: profile.profile_id,
                  player_id: profile.id,
                  date_up: "14.12.2023",
                },
                function () {}
              );
            }
            break;
          }
        }
      }
    }
  );
  send(res, 200, { status: "ok" });
});

router.post(
  "/games/webhooks",
  passport.authenticate("api", { session: false }),
  (req, res, next) => {
    log.warn("Got Games webhook:", req.body);
    send(res, 200, {});
  }
);

router.post(
  "/games/result",
  passport.authenticate("api", { session: false }),
  (req, res, next) => {
    log.warn("Got Games game request:", req.body);
    let results = [
      { status: "pending" },
      { status: "pending" },
      { status: "ready", url: "https://drawlinks.com/s/aoc4Te2g0y" },
    ];

    send(res, 200, _.sample(results));
  }
);

module.exports = router;
