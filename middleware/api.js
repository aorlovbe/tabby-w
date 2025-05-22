let axios = require("axios");
let log = require("../services/bunyan").log;
let _ = require("lodash");
let moment = require("moment");
const momentTimezone = require("moment-timezone");
let redis = require("../services/redis").redisclient;
let bulk = require("../services/bulk");
let sha = require("../services/sha");
let jws = require("../services/jws");
let aes_basic = require("../services/aes");
let nanoid = require("../services/nanoid");
let producer = require("../services/producer");
let Game = require("../api/games");
let User = require("../api/users");
let Counters = require("../api/counters");
let Leaderboards = require("../api/leaderboard");
let Achievements = require("../api/achievements");
let Rewards = require("../api/rewards");
const requestIp = require("request-ip");
const send = require("@polka/send-type");
const timeZone = require("moment-timezone");
const Profiles = require("../api/profiles");
const Leaderboard = require("../api/leaderboard");
const accelera = require("../services/producer");
const crate = require("../services/crateio");
const async = require("async");

class API {
  static getProfile(req, res, next) {
    Profiles.get(req.body.profile_id, function (err, profiles) {
      req.body.profile = profiles;
      return next();
    });
  }

  static mobileChecksum(req, res, next) {
    //Pushing to accelera
    req.body.x_api_key = req.headers["x-api-key"];
    req.body.game_id = req.params.game_id;

    log.info("Requesting token:", req.body.x_api_key, req.body.login);
    if (req.body.x_api_key === undefined || req.body.login === undefined)
      return send(res, 500, { status: "failed" });
    req.body.ctn = req.body.login;
    // Need to add checksum
    // sha1(CTN;соль)
    Game.findwithprivate(req, function (err, game) {
      let checksum = sha.encrypt(req.body.login.toString() + game.private.salt);
      if (checksum.toUpperCase() === req.body.x_api_key.toUpperCase()) {
        log.info("Checksum passed");
        next();
      } else {
        log.error(
          "Failed on checksum: got / calculated",
          req.body.x_api_key,
          checksum,
          req.body.login.toString() + game.private.salt
        );
        return send(res, 500, { status: "failed" });
      }
    });
  }

  static publish(id, event, context, callback) {
    //Pushing to accelera
    accelera
      .publishTrigger(id, event, context)
      .then(function () {
        log.debug("Trigger was published:", id, event);
        callback();
      })
      .catch((e) => {
        log.error("Failed to publish trigger:", event, e);
        callback(true);
      });
  }

  static markVisit(req, res, next) {
    let account = {
      timestamp: Math.floor(new Date()),
      id: "",
      username: "",
      email: "",
      name: "",
      surname: "",
      gender: "",
      social: "",
      status: "visit",
      game_id: req.body.game.game_id,
      fingerprint: JSON.stringify(req.body.fingerprint),
      date: moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD"),
      time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
      datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
        "YYYY-MM-DD HH:mm:ss"
      ),
    };

    //Updating auth table
    bulk.store("auth", JSON.stringify(account), function () {});

    //Publish trigger
    if (req.body.fingerprint !== undefined) {
      API.publish(
        req.body.fingerprint.fingerprint,
        "visit",
        req.body.fingerprint,
        function () {}
      );
    }

    //Check if game is closed
    if (req.body.game.private.closed === true)
      return send(res, 500, { status: "failed" });

    next();
  }

  static getGame(req, res, next) {
    Game.findwithprivate(req, function (err, game) {
      if (err || game.private.closed === true)
        return send(res, 500, { status: "failed" });
      req.body.game = game;
      return next();
    });
  }

  static checkDecision(req, res, next) {
    if (req.body.decision === "accept") {
      log.info("Got accept decision:", req.body.profile_id, req.body.service);
      next();
    } else {
      log.info("Got reject decision:", req.body.profile_id, req.body.service);
      return send(res, 200, { status: "ok" });
    }
  }

  static isWhitelist(req, res, next) {
    if (req.body.game.development === true) {
      if (req.body.game.whitelist !== undefined) {
        if (req.body.game.whitelist.includes(req.body.ctn) === true) {
          next();
        } else {
          log.warn("Whitelist is active, forbidden:", req.body.ctn);
          return send(res, 500, { status: "whitelist" });
        }
      } else {
        next();
      }
    } else {
      next();
    }
  }

  static TriesCheckup(req, res, next) {
    // Get the current timestamp in milliseconds
    let currentTimestamp = Math.floor(new Date().getTime());
    let currentDate = moment(new Date()).format("YYYY-MM-DD");
    // Convert the timestamp to hours and minutes
    let currentTime = new Date(currentTimestamp);
    let currentHours = currentTime.getHours();
    let currentMinutes = currentTime.getMinutes();
    const morningStartHour = 0;
    const morningEndHour = 7;
    const afternoonStartHour = 8;
    const afternoonEndHour = 15;
    const eveningStartHour = 16;
    const eveningEndHour = 23;

    if (req.body.counters.last_checkup !== undefined) {
      // Define the time periods in hours (in 24-hour format)
      let periods = JSON.parse(req.body.counters.last_checkup);
      // Determine the current time period
      if (
        (currentHours >= morningStartHour && currentHours < morningEndHour) ||
        (currentHours === morningEndHour && currentMinutes < 60)
      ) {
        log.info(
          "The current time is in the morning period (00:00 - 07:59)",
          currentTime
        );
        if (periods.period !== 1) {
          log.info("New checkup for", req.body.profile_id, currentDate, 1);
          checkup(1, currentDate);
        } else {
          if (periods.date !== currentDate) {
            log.info("New checkup for", req.body.profile_id, currentDate, 1);
            checkup(1, currentDate);
          } else {
            log.info("No new checkups for", req.body.profile_id, currentDate);
            next();
          }
        }
      } else if (
        (currentHours >= afternoonStartHour &&
          currentHours < afternoonEndHour) ||
        (currentHours === afternoonEndHour && currentMinutes < 60)
      ) {
        log.info(
          "The current time is in the afternoon period (08:00 - 15:59)",
          currentTime
        );
        if (periods.period !== 2) {
          log.info("New checkup for", req.body.profile_id, currentDate, 2);
          checkup(2, currentDate);
        } else {
          if (periods.date !== currentDate) {
            log.info("New checkup for", req.body.profile_id, currentDate, 2);
            checkup(2, currentDate);
          } else {
            log.info("No new checkups for", req.body.profile_id, currentDate);
            next();
          }
        }
      } else if (
        (currentHours >= eveningStartHour && currentHours < eveningEndHour) ||
        (currentHours === eveningEndHour && currentMinutes < 60)
      ) {
        log.info(
          "The current time is in the evening period (16:00 - 23:59)",
          currentTime
        );
        if (periods.period !== 3) {
          log.info("New checkup for", req.body.profile_id, currentDate, 3);
          checkup(3, currentDate);
        } else {
          if (periods.date !== currentDate) {
            log.info("New checkup for", req.body.profile_id, currentDate, 3);
            checkup(3, currentDate);
          } else {
            log.info("No new checkups for", req.body.profile_id, currentDate);
            next();
          }
        }
      } else {
        log.info("The current time is not within any defined period.");
        log.info(
          "New default checkup for",
          req.body.profile_id,
          currentDate,
          1
        );
        checkup(1, currentDate);
      }
    } else {
      if (
        (currentHours >= morningStartHour && currentHours < morningEndHour) ||
        (currentHours === morningEndHour && currentMinutes < 60)
      ) {
        log.info(
          "The current time is in the morning period (00:00 - 07:59)",
          currentTime
        );
        log.info("New checkup for", req.body.profile_id, currentDate, 1);
        checkup(1, currentDate);
      } else if (
        (currentHours >= afternoonStartHour &&
          currentHours < afternoonEndHour) ||
        (currentHours === afternoonEndHour && currentMinutes < 60)
      ) {
        log.info(
          "The current time is in the afternoon period (08:00 - 15:59)",
          currentTime
        );
        log.info("New checkup for", req.body.profile_id, currentDate, 2);
        checkup(2, currentDate);
      } else if (
        (currentHours >= eveningStartHour && currentHours < eveningEndHour) ||
        (currentHours === eveningEndHour && currentMinutes < 60)
      ) {
        log.info(
          "The current time is in the evening period (16:00 - 23:59)",
          currentTime
        );
        log.info("New checkup for", req.body.profile_id, currentDate, 3);
        checkup(3, currentDate);
      } else {
        log.info("The current time is not within any defined period.");
        log.info(
          "New default checkup for",
          req.body.profile_id,
          currentDate,
          1
        );
        checkup(1, currentDate);
      }
    }

    function checkup(period, date) {
      let stars =
        req.body.counters.stars !== undefined
          ? parseInt(req.body.counters.stars)
          : 0;
      if (stars === 90) {
        let add_tries = {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
          name: "tries",
          value: 1,
        };

        //Pushing to accelera
        accelera
          .publishTrigger(req.body.profile_id, "birthday-period", {
            game_id: req.body.game.game_id,
            profile_id: req.body.profile_id,
            player_id: req.body.player_id,
            tries: 1,
            ending: "попытка",
          })
          .then(function () {
            log.info(
              "Trigger was published:",
              "birthday-period",
              req.body.profile_id
            );
          })
          .catch((e) => {
            log.error("Failed to publish trigger:", e);
          });

        Counters.modify({ body: add_tries }, function () {
          //Update last checkup data
          let last_checkup = {
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            name: "last_checkup",
            value: JSON.stringify({ period: period, date: date }),
          };

          Counters.create({ body: last_checkup }, function () {
            log.info("Checkup was updated:", JSON.stringify(last_checkup), 1);
            next();
          });
        });
      } else {
        let add_tries = {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
          name: "tries",
          value: 5,
        };

        //Pushing to accelera
        accelera
          .publishTrigger(req.body.profile_id, "birthday-period", {
            game_id: req.body.game.game_id,
            profile_id: req.body.profile_id,
            player_id: req.body.player_id,
            tries: 5,
            ending: "попыток",
          })
          .then(function () {
            log.info(
              "Trigger was published:",
              "birthday-period",
              req.body.profile_id
            );
          })
          .catch((e) => {
            log.error("Failed to publish trigger:", e);
          });

        Counters.modify({ body: add_tries }, function () {
          //Update last checkup data
          let last_checkup = {
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            name: "last_checkup",
            value: JSON.stringify({ period: period, date: date }),
          };

          Counters.create({ body: last_checkup }, function () {
            log.info("Checkup was updated:", JSON.stringify(last_checkup), 5);
            next();
          });
        });
      }
    }
  }

  static getGameByQueryParams(req, res, next) {
    req.body.game_id = req.params.game_id;
    Game.findwithprivate(req, function (err, game) {
      if (err) {
        log.error("Game not found:", req.params.game_id, err);
        return send(res, 500, { status: "failed" });
      }
      req.body.game = game;
      return next();
    });
  }

  static getGameWithoutPrivate(req, res, next) {
    Game.find(req, function (err, game) {
      if (err) return send(res, 500, { status: "failed" });

      req.body.game = game;
      return next();
    });
  }

  static checkConfirmationCode(req, res, next) {
    if (
      [
        "79022589631",
        "79685836888",
        "79022399778",
        "79685706335",
        "79605821928",
        "79602831832",
      ].includes(req.body.ctn) === true
    ) {
      log.warn(
        "Failed for ctn:",
        req.body.game.game_id,
        req.body.ctn,
        req.body.code.toString()
      );
      return send(res, 500, { status: "failed" });
    } else {
      if (req.body.ctn.toString().length !== 11) {
        API.publish(
          req.body.ctn,
          "fraud",
          {
            game_id: req.body.game.game_id,
            ctn: req.body.ctn.toString(),
            code: req.body.code.toString(),
          },
          function () {}
        );
        log.warn(
          "Fraud detected:",
          req.body.ctn.toString(),
          req.body.code.toString()
        );
        return send(res, 200, { status: "..." });
      } else {
        let salt =
          "8242m4aaafs524288da-sd88242m4d9a8242m49r-sdU7HTuKab78G-~ns~##";
        if (
          sha.encrypt(req.body.code.toString() + salt) !== req.body.checksum
        ) {
          //Publish trigger
          API.publish(
            req.body.ctn,
            "sms_auth_failed",
            {
              game_id: req.body.game.game_id,
              ctn: req.body.ctn,
              code: req.body.code,
              checksum: req.body.checksum,
            },
            function () {}
          );
          return send(res, 500, { status: "failed" });
        } else {
          //Publish trigger
          API.publish(
            req.body.ctn,
            "sms_auth_passed",
            {
              game_id: req.body.game.game_id,
              ctn: req.body.ctn,
              code: req.body.code,
              checksum: req.body.checksum,
            },
            function () {}
          );
          req.body.authorized = true;
          req.body.channel = "web";
          next();
        }
      }
    }
  }

  static Authorize(req, res, next) {
    //Authorization and registration by CNT
    if (
      req.body.ctn === undefined ||
      req.body.ctn.toString().split("")[0] !== "7"
    )
      return send(res, 500, { status: "failed" });

    //let ip = requestIp.getClientIp(req).toString();
    //if (['217.118.86.110','217.118.86.106','217.118.86.105'].includes(ip) === true) return send(res, 500, { status: 'failed' });

    log.debug(
      "Creating / updating new user & game profile in Accelera API",
      req.body.ctn
    );
    req.body.player_id = req.body.ctn.toString();

    let user = {
      id: req.body.player_id,
      username: "",
      email: "",
      password: "",
      name: "",
      surname: "",
      gender: "",
      fingerprint: req.body.fingerprint,
      game_id: req.body.game.game_id,
      social: req.body.channel === undefined ? "mobile" : req.body.channel,
    };

    User.register(user, (err, created, account) => {
      if (err) {
        log.error(
          "Creating / updating new user in Accelera API was failed!",
          req.body.player_id,
          err
        );
        return send(res, 500, { status: "failed" });
      }

      req.body.status = created;
      req.user = account;

      Profiles.register(req, function (err, profile) {
        if (err) {
          //Storing to clickhouse
          log.error(
            "Creating / updating new profile in Accelera API was failed!",
            req.body.player_id,
            err
          );

          let event = {
            event: "accelera-api",
            page: req.body.type,
            status: "failed",
            additional:
              req.body.x_api_key !== undefined ? req.body.x_api_key : "",
            game_id: req.body.game.game_id,
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

          return send(res, 500, { status: "failed" });
        } else {
          profile.player_id = req.body.player_id;
          profile.authorized = req.body.authorized;
          profile.fingerprint =
            req.body.fingerprint === undefined ? "" : req.body.fingerprint;
          profile.ip = requestIp.getClientIp(req).toString();
          profile.token_created = Math.floor(new Date());
          req.body.connection_ip = profile.ip;
          req.body.jwt = jws.encrypt(profile);
          req.body.profile_id = profile.profile_id;

          //Storing to clickhouse
          let event = {
            event: "accelera-api",
            page: req.body.type,
            jwt: req.body.jwt,
            status: "succeed",
            context: req.body.utm !== undefined ? req.body.utm : "",
            details: profile.ip,
            additional:
              req.body.x_api_key !== undefined
                ? "mobile"
                : JSON.stringify(profile.fingerprint),
            game_id: req.body.game.game_id,
            gifts: [
              req.body.channel === undefined ? "mobile" : req.body.channel,
            ],
            player_id:
              req.body.player_id === undefined
                ? ""
                : req.body.player_id.toString(),
            profile_id: profile.profile_id.toString(),
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

          next();
        }
      });
    });
  }

  static isBlocked(req, res, next) {
    Profiles.is_block(req.body.profile_id, function (blocked) {
      if (blocked) {
        log.warn(
          "Profile is blocked:",
          req.body.profile_id,
          req.body.game.game_id
        );
        return send(res, 500, { status: "blocked" });
      } else {
        return next();
      }
    });
  }

  static isBlockedClient(req, res, next) {
    Profiles.is_block(req.body.player_id, function (blocked) {
      if (blocked) {
        log.warn("CTN is blocked:", req.body.player_id, req.body.game.game_id);
        return send(res, 500, { status: "blocked" });
      } else {
        return next();
      }
    });
  }

  static isBlockedCTN(req, res, next) {
    Profiles.is_block(req.body.ctn, function (blocked) {
      if (blocked) {
        log.warn("CTN is blocked:", req.body.ctn, req.body.game.game_id);
        return send(res, 500, { status: "blocked" });
      } else {
        return next();
      }
    });
  }

  static isBlockedIP(req, res, next) {
    let ip = requestIp.getClientIp(req).toString();
    req.body.request_ip = ip;
    Profiles.is_block(ip, function (blocked) {
      log.info("Checking IP", ip, blocked);
      if (blocked) {
        log.error("Blocked by IP:", ip, req.body.game.game_id);
        return send(res, 500, { status: "blocked" });
      } else {
        return next();
      }
    });
  }

  static isBlockedIPfromToken(req, res, next) {
    let ip = req.body.decrypted_token.ip;
    Profiles.is_block(ip, function (blocked) {
      if (blocked) {
        log.error("Blocked by IP (from token):", ip, req.body.game.game_id);
        return send(res, 500, { status: "blocked" });
      } else {
        return next();
      }
    });
  }

  static isBanned(req, res, next) {
    Profiles.is_ban(req.body.profile_id, function (blocked) {
      if (blocked) {
        return send(res, 500, { status: "banned" });
      } else {
        return next();
      }
    });
  }

  static isGameClosed(req, res, next) {
    if (req.body.game.private.closed) {
      return send(res, 500, { status: "closed" });
    } else {
      return next();
    }
  }

  static Counters(req, res, next) {
    log.info("Getting counters for", req.body.player_id, "in Accelera API");

    Counters.findbyprofile(req, function (err, counters) {
      //Storing to clickhouse
      let event = {
        event: "accelera-api",
        page: "counters",
        status: "requested",
        game_id: "tabby",
        details: JSON.stringify(counters),
        player_id:
          req.body.player_id === undefined ? "" : req.body.player_id.toString(),
        profile_id: req.body.profile_id,
        timestamp: Math.floor(new Date()),
        date: moment(new Date()).format("YYYY-MM-DD"),
        time: moment(new Date()).format("HH:mm"),
        datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
          "YYYY-MM-DD HH:mm:ss"
        ),
      };

      /*bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
                if (err) {
                    log.error('Error while storing webhooks messages for Clickhouse bulk:', err);
                }
            });*/

      req.body.counters = counters === null ? {} : counters;

      next();
    });
  }

  static Leaderboard(req, res, next) {
    log.info("Getting leaderboard for", req.body.profile_id, "in Accelera API");

    req.body.system = req.body.game.game_id;
    req.body.name = req.body.counter;

    Leaderboards.get(req, function (err, callback) {
      req.body.leaderboard = callback;
      next();
    });
  }

  static AcceleraCoupons(req, res, next) {
    redis.hget(
      "platform:accelera:coupons:info",
      "coupons",
      function (err, coupons) {
        if (err) {
          req.body.accelera_coupons = {};
          next();
        } else {
          req.body.accelera_coupons = JSON.parse(coupons);
          next();
        }
      }
    );
  }

  static blockProfile(req, next) {
    log.warn("Blocking profile:", req.body.profile_id, "in Accelera API");

    Profiles.block(req.body.profile_id, function (err, done) {
      if (!err) {
        //Storing to clickhouse
        let event = {
          event: "accelera-api",
          page: "profiles",
          status: "blocked",
          profile_id: req.body.profile_id,
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

        next();
      } else {
        //Storing to clickhouse
        let event = {
          event: "accelera-api",
          page: "profiles",
          status: "block-failed",
          profile_id: req.body.profile_id,
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
        next(true);
      }
    });
  }

  static unblockProfile(req, next) {
    log.warn("Unblocking profile:", req.body.profile_id, "in Accelera API");

    Profiles.unblock(req.body.profile_id, function (err, done) {
      if (!err) {
        //Storing to clickhouse
        let event = {
          event: "accelera-api",
          page: "profiles",
          status: "unblocked",
          profile_id: req.body.profile_id,
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

        next();
      } else {
        //Storing to clickhouse
        let event = {
          event: "accelera-api",
          page: "profiles",
          status: "unblock-failed",
          profile_id: req.body.profile_id,
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
        next(true);
      }
    });
  }

  static reloadDailyLeaderboard(req, res, next) {
    log.debug(
      "Processing leaderboard reload data:",
      req.body.game_id,
      req.body.profile_id
    );

    Profiles.get(req.body.profile_id, function (err, profile) {
      if (err || Object.keys(profile).length === 0) {
        log.error("Profile not found:", profile, err);
        next();
      } else {
        let date = moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD");

        //Check to reload profile score once a day
        if (profile.last_signin_date !== date) {
          //Updating reload data
          Profiles.modify(
            {
              body: {
                last_signin_date: date,
                profile_id: req.body.profile_id,
              },
            },
            function () {
              //Storing points to user ID (leaderboard by user)
              Leaderboard.setDaily(
                {
                  body: {
                    system: req.body.game_id,
                    name: "points",
                    value:
                      req.body.game.private.multiplayer
                        .daily_leaderboard_points,
                    profile_id: profile.id,
                  },
                },
                function () {
                  log.info(
                    "Leaderboard is updated by:",
                    req.body.game.private.multiplayer.daily_leaderboard_points
                  );
                  next();
                }
              );
            }
          );
        } else {
          log.info(
            "Player already signed in today, daily leaderboard cannot be reloaded:",
            date
          );
          next();
        }
      }
    });
  }

  static getCrateLevelsbyMetka(req, res, next) {
    log.debug("Getting optin levels:", req.body.game_id, req.body.profile_id);

    if (
      ["253.229.26.17", "88.198.56.84", "34.66.9.17"].includes(
        req.body.decrypted_token.ip
      ) === true
    )
      return send(res, 500, { status: "failed" });

    redis.hget(
      "platform:birthday:optins",
      req.body.decrypted_token.metka,
      function (err, levels) {
        if (err) {
          log.error("Failed to get levels from redis:", err);
          req.body.levels = {};
          next();
        } else {
          log.info("Got levels info:", levels);

          if (req.body.decrypted_token.metka.includes("default") === true) {
            log.warn(
              "Default levels will be replied:",
              req.body.decrypted_token.metka,
              req.body.decrypted_token.ip
            );
            req.body.levels = {
              ctn: req.body.player_id,
              metka: req.body.decrypted_token.metka,
              default_1_place_1: "",
              default_1_place_2: "",
              default_1_place_3: "",
              default_1_place_4: "",
              default_2_place_1: "",
              default_2_place_2: "",
              default_2_place_3: "",
              default_2_place_4: "",
              welcome_place_1: "b-1",
              welcome_place_2: "b-18",
              welcome_place_3: "",
              welcome_place_4: "",
              welcome_place_5: "",
              welcome_place_6: "",
              welcome_place_7: "",
              welcome_place_8: "",
              welcome_place_9: "",
              welcome_place_10: "",
              game_place_1: "b-19",
              game_place_2: "b-17",
              game_place_3: "b-22",
              game_place_4: "b-21",
              game_place_5: "b-41",
              game_place_6: "b-23",
              game_place_7: "b-3",
              game_place_8: "b-42",
              game_place_9: "b-29",
              game_place_10: "b-45",
              game_place_11: "b-40",
              game_place_12: "b-35",
              game_place_13: "b-36",
              game_place_14: "b-13",
              game_place_15: "b-25",
              game_place_16: "b-34",
              game_place_17: "b-4",
              game_place_18: "b-27",
              game_place_19: "b-6",
              game_place_20: "b-20",
              game_place_21: "b-14",
              game_place_22: "b-37",
              game_place_23: "b-46",
              game_place_24: "b-43",
              game_place_25: "b-30",
              game_place_26: "b-28",
              game_place_27: "b-7",
              game_place_28: "b-15",
              game_place_29: "b-44",
              game_place_30: "b-47",
              task_1_place1: "b-16",
              task_1_place2: "b-38",
              task_1_place3: "",
              task_1_place4: "",
              task_1_place5: "",
              task_1_place6: "",
              task_2_place1: "",
              task_2_place2: "",
              task_2_place3: "",
              task_2_place4: "",
              task_2_place5: "",
              task_2_place6: "",
            };
            next();
          } else {
            req.body.levels = JSON.parse(levels);
            next();
          }
        }
      }
    );
  }

  static createorGetWaveRewards(req, res, next) {
    log.debug(
      "Getting waves rewards or create them if not exists:",
      req.body.decrypted_token.metka,
      req.body.profile_id
    );

    let current_timestamp = Math.floor(new Date());
    let second_wave = req.body.game.private.second_wave;
    let wave2_is_now = current_timestamp >= second_wave ? true : false;

    redis
      .multi()
      .hget("platform:wave-1:metka", req.body.decrypted_token.metka)
      .hget("platform:wave-2:metka", req.body.decrypted_token.metka)
      .hget("platform:birthday:optins", req.body.decrypted_token.metka)
      .exec(function (err, wave_rewards) {
        if (err) return res.end(JSON.stringify({ wave1: [], wave2: [] }));

        //User already was here, checking wave
        if (wave_rewards[0] !== null) {
          //Got a user already optinned and have wave #1 rewards
          log.info(
            "Got a user already optinned and have wave #1 rewards",
            req.body.decrypted_token.metka
          );
          //Checking second wave
          if (wave_rewards[1] !== null) {
            //Got a user already optinned and have wave #2 rewards, returning
            req.body.waves = {
              wave1: JSON.parse(wave_rewards[0]),
              wave2: JSON.parse(wave_rewards[1]),
            };
            next();
          } else {
            //Check wave2 date is now
            if (wave2_is_now) {
              //New wave was started, need to give wave 2 rewards
              log.info("New wave was started, need to give wave 2 rewards");
              let user = JSON.parse(wave_rewards[2]); // raw data from file
              let rewards = {
                default_2_place_1: user.default_2_place_1,
                default_2_place_2: user.default_2_place_2,
                default_2_place_3: user.default_2_place_3,
                default_2_place_4: user.default_2_place_4,
              };

              //Issuing rewards
              issueRewards("2", rewards, req, function (err, processed) {
                if (err) {
                  log.error("Error on creating reward for wave #2:", err);
                  req.body.waves = { wave1: [], wave2: [] };
                  next();
                } else {
                  //Storing to redis
                  redis.hset(
                    "platform:wave-2:metka",
                    req.body.decrypted_token.metka,
                    JSON.stringify(processed.wave2),
                    function () {}
                  );

                  //Sending events to create rewards by metka
                  for (let i in processed.wave2) {
                    //Sending session result event to a flow
                    API.publish(
                      req.body.decrypted_token.metka,
                      "metka-reward",
                      {
                        profile_id: req.body.decrypted_token.metka,
                        game_id: req.body.game.game_id,
                        metka: req.body.decrypted_token.metka,
                        wave: "2",
                        player_id: user.ctn,
                        reward: processed.wave2[i],
                      },
                      function () {}
                    );
                  }

                  req.body.waves = {
                    wave1: JSON.parse(wave_rewards[0]),
                    wave2: processed.wave2,
                  };
                  next();
                }
              });
            } else {
              //Its still wave #1, too early for wave #2
              log.info("Its still wave #1, too early for wave #2");
              req.body.waves = {
                wave1: JSON.parse(wave_rewards[0]),
                wave2: [],
              };
              next();
            }
          }
        } else {
          //New user by metka, searching in crate.io
          crate.getRewardsByMetka(
            req.body.decrypted_token.metka,
            function (err, user) {
              if (user === undefined) {
                log.warn(
                  "User by metka in not found, maybe fake data:",
                  req.body.decrypted_token.metka
                );
                req.body.waves = { wave1: [], wave2: [] };
                next();
              } else {
                log.info(
                  "Got user from crate.io:",
                  JSON.stringify(user),
                  current_timestamp,
                  "/ wave #2 time is:",
                  second_wave
                );

                if (wave2_is_now) {
                  log.info(
                    "New wave was started, need to give wave 1+2 rewards"
                  );

                  let rewards = {
                    default_1_place_1: user.default_1_place_1,
                    default_1_place_2: user.default_1_place_2,
                    default_1_place_3: user.default_1_place_3,
                    default_1_place_4: user.default_1_place_4,
                    default_2_place_1: user.default_2_place_1,
                    default_2_place_2: user.default_2_place_2,
                    default_2_place_3: user.default_2_place_3,
                    default_2_place_4: user.default_2_place_4,
                  };

                  issueRewards("all", rewards, req, function (err, processed) {
                    if (err) {
                      log.error("Error on creating reward for all waves:", err);
                      req.body.waves = { wave1: [], wave2: [] };
                      next();
                    } else {
                      res.end(
                        JSON.stringify({
                          wave1: processed.wave1,
                          wave2: processed.wave2,
                        })
                      );
                      //Storing to redis
                      //Storing to redis
                      redis.hset(
                        "platform:wave-1:metka",
                        req.body.decrypted_token.metka,
                        JSON.stringify(processed.wave1),
                        function () {}
                      );
                      redis.hset(
                        "platform:wave-2:metka",
                        req.body.decrypted_token.metka,
                        JSON.stringify(processed.wave2),
                        function () {}
                      );

                      //Sending events to create rewards by metka
                      for (let i in processed.wave1) {
                        //Sending session result event to a flow
                        API.publish(
                          req.body.decrypted_token.metka,
                          "metka-reward",
                          {
                            profile_id: req.body.decrypted_token.metka,
                            game_id: req.body.game.game_id,
                            player_id: user.ctn,
                            metka: req.body.decrypted_token.metka,
                            wave: "1",
                            reward: processed.wave1[i],
                          },
                          function () {}
                        );
                      }

                      //Sending events to create rewards by metka
                      for (let i in processed.wave2) {
                        //Sending session result event to a flow
                        API.publish(
                          req.body.decrypted_token.metka,
                          "metka-reward",
                          {
                            profile_id: req.body.decrypted_token.metka,
                            game_id: req.body.game.game_id,
                            player_id: user.ctn,
                            metka: req.body.decrypted_token.metka,
                            wave: "2",
                            reward: processed.wave2[i],
                          },
                          function () {}
                        );
                      }

                      req.body.waves = {
                        wave1: processed.wave1,
                        wave2: processed.wave2,
                      };
                      next();
                    }
                  });
                } else {
                  let rewards = {
                    default_1_place_1: user.default_1_place_1,
                    default_1_place_2: user.default_1_place_2,
                    default_1_place_3: user.default_1_place_3,
                    default_1_place_4: user.default_1_place_4,
                  };

                  issueRewards("1", rewards, req, function (err, processed) {
                    if (err) {
                      log.error("Error on creating reward for all waves:", err);
                      req.body.waves = { wave1: [], wave2: [] };
                      next();
                    } else {
                      //Storing to redis
                      redis.hset(
                        "platform:wave-1:metka",
                        req.body.decrypted_token.metka,
                        JSON.stringify(processed.wave1),
                        function () {}
                      );

                      //Sending events to create rewards by metka
                      for (let i in processed.wave1) {
                        //Sending session result event to a flow
                        API.publish(
                          req.body.decrypted_token.metka,
                          "metka-reward",
                          {
                            profile_id: req.body.decrypted_token.metka,
                            game_id: req.body.game.game_id,
                            metka: req.body.decrypted_token.metka,
                            player_id: user.ctn,
                            wave: "1",
                            reward: processed.wave1[i],
                          },
                          function () {}
                        );
                      }

                      req.body.waves = { wave1: processed.wave1, wave2: [] };
                      next();
                    }
                  });
                }
              }
            }
          );
        }
      });

    function issueRewards(period, rewards, req, callback) {
      //1 - only for wave 1
      //2 - only for wave 2
      //all - all waves
      let partners = req.body.game.rewards;
      let wave1 = [];
      let wave2 = [];

      log.info("Issuing rewards for wave:", period, rewards);

      async.forEachOf(
        rewards,
        function (value, key, continue_process) {
          if (value !== "") {
            let partner = _.find(partners, { id: value });
            log.info("Searching for:", value, partner);

            if (partner.status === "not-active") {
              log.warn("Partner is not active, postponed:", value);
              continue_process();
            } else {
              switch (partner.activation_type) {
                case "unique": {
                  //Getting coupon
                  getCoupon(partner.promocode[0], function (err, promocode) {
                    if (err) {
                      //No code in stack or error
                      continue_process();
                    } else {
                      partner.coupon = promocode;
                      delete partner["promocode"];

                      //Which wave?
                      if (period === "1") {
                        wave1.push(_.cloneDeep(partner));
                      } else if (period === "2") {
                        wave2.push(_.cloneDeep(partner));
                      } else {
                        if (key.includes("default_1") === true) {
                          wave1.push(_.cloneDeep(partner));
                        } else {
                          wave2.push(_.cloneDeep(partner));
                        }
                      }
                      continue_process();
                    }
                  });
                  break;
                }

                case "mass_link": {
                  partner.coupon = _.sample(partner.promocode);
                  delete partner["promocode"];

                  //Which wave?
                  if (period === "1") {
                    wave1.push(_.cloneDeep(partner));
                  } else if (period === "2") {
                    wave2.push(_.cloneDeep(partner));
                  } else {
                    if (key.includes("default_1") === true) {
                      wave1.push(_.cloneDeep(partner));
                    } else {
                      wave2.push(_.cloneDeep(partner));
                    }
                  }

                  continue_process();
                  break;
                }

                case "unique_link": {
                  getCoupon(partner.promocode[0], function (err, promocode) {
                    if (err) {
                      //No code in stack or error
                      log.error("Error while getting coupon:", err);
                      continue_process();
                    } else {
                      partner.link = partner.link.replace(
                        "{{promocode}}",
                        promocode
                      );
                      delete partner["promocode"];

                      //Which wave?
                      if (period === "1") {
                        wave1.push(_.cloneDeep(partner));
                      } else if (period === "2") {
                        wave2.push(_.cloneDeep(partner));
                      } else {
                        if (key.includes("default_1") === true) {
                          wave1.push(_.cloneDeep(partner));
                        } else {
                          wave2.push(_.cloneDeep(partner));
                        }
                      }
                      next();
                    }
                  });

                  break;
                }

                case "unique_nolink": {
                  //Getting coupon
                  getCoupon(partner.promocode[0], function (err, promocode) {
                    if (err) {
                      //No code in stack or error
                      log.error(
                        "Error while getting coupon:",
                        partner.promocode[0],
                        err
                      );
                      continue_process();
                    } else {
                      partner.coupon = promocode;
                      delete partner["promocode"];

                      //Which wave?
                      if (period === "1") {
                        wave1.push(_.cloneDeep(partner));
                      } else if (period === "2") {
                        wave2.push(_.cloneDeep(partner));
                      } else {
                        if (key.includes("default_1") === true) {
                          wave1.push(_.cloneDeep(partner));
                        } else {
                          wave2.push(_.cloneDeep(partner));
                        }
                      }
                      continue_process();
                    }
                  });

                  break;
                }

                case "mass_nolink": {
                  partner.coupon = _.sample(partner.promocode);
                  delete partner["promocode"];

                  //Which wave?
                  if (period === "1") {
                    wave1.push(_.cloneDeep(partner));
                  } else if (period === "2") {
                    wave2.push(_.cloneDeep(partner));
                  } else {
                    if (key.includes("default_1") === true) {
                      wave1.push(_.cloneDeep(partner));
                    } else {
                      wave2.push(_.cloneDeep(partner));
                    }
                  }

                  continue_process();

                  break;
                }

                default: {
                  delete partner["promocode"];

                  //Which wave?
                  if (period === "1") {
                    wave1.push(_.cloneDeep(partner));
                  } else if (period === "2") {
                    wave2.push(_.cloneDeep(partner));
                  } else {
                    if (key.includes("default_1") === true) {
                      wave1.push(_.cloneDeep(partner));
                    } else {
                      wave2.push(_.cloneDeep(partner));
                    }
                  }

                  continue_process();
                  break;
                }
              }
            }
          } else {
            //No gift in stack for this person
            log.info("No gift in stack for this person, postponed");
            continue_process();
          }
        },
        function (err) {
          // if any of the file processing produced an error, err would equal that error
          if (err) {
            callback(true, err);
          } else {
            log.info("All gifts have been processed successfully");
            let processed = { wave1: wave1, wave2: wave2 };
            callback(false, processed);
          }
        }
      );
    }

    function getCoupon(stack, done) {
      //log.warn('Generating fake coupon!')
      //done(false, Math.floor(new Date()));

      redis.lpop("platform:coupons:" + stack, function (err, promocode) {
        if (err || promocode === null) {
          done(true);
        } else {
          done(false, promocode);
        }
      });
    }
  }

  static createSession(req, res, next) {
    //Checking if tries is enough and charge from counter. If its '' so game is free
    if (req.body.game.private.closed === true)
      return send(res, 500, { status: "failed" });

    if (req.body.game.private.sessions.changefrom !== "") {
      if (req.body.counters[req.body.game.private.sessions.changefrom] > 0) {
        let player = {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
          name: req.body.game.private.sessions.changefrom,
          value: -1,
        };
        Counters.modify({ body: player }, function (err, updates) {
          if (err || updates[req.body.game.private.sessions.changefrom] < 0) {
            log.error(
              "Wow, player wants to play without balance. Will be kicked in Accelera",
              req.body.profile_id,
              req.body.game.game_id,
              JSON.stringify(updates),
              err
            );

            //Block profile
            Profiles.block(req.body.profile_id, function (err, ok) {});

            //Storing to clickhouse
            let event = {
              event: "accelera-api",
              page: "sessions",
              status: "failed-to-create",
              game_id: req.body.game.game_id,
              profile_id: req.body.profile_id,
              player_id: req.body.player_id,
              context:
                updates[req.body.game.private.sessions.changefrom].toString(),
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

            API.publish(
              req.body.profile_id,
              "ban",
              {
                profile_id: req.body.profile_id,
                game_id: req.body.game.game_id,
                reason:
                  "Попытка игры с отрицательным балансом попыток (после списания): " +
                  updates[req.body.game.private.sessions.changefrom] +
                  " / " +
                  req.body.profile_id,
              },
              function () {}
            );

            return send(res, 403, { status: "balance" });
          } else {
            req.body.session = nanoid.getmax();
            //Storing to clickhouse
            let event = {
              event: "accelera-api",
              page: "sessions",
              status: "created",
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              profile_id: req.body.profile_id,
              context: req.body.session,
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

            //Storing session to Redis
            Game.storeSession(
              req.body.session,
              req.body.profile_id,
              req.body.game,
              function (err) {
                if (err) return send(res, 500, { status: "failed" });
                next();
              }
            );
          }
        });
      } else {
        return send(res, 403, { status: "balance" });
      }
    } else {
      //Storing session & level to Redis
      let level = req.body.level === undefined ? 0 : req.body.level;
      let current_time = Math.floor(new Date());

      let leveldata = req.body.game.levels[level - 1];
      if (current_time >= leveldata.timestamp) {
        req.body.session = nanoid.getmax();
        //Storing to clickhouse
        let event = {
          event: "accelera-api",
          page: "sessions",
          status: "created",
          game_id: req.body.game.game_id,
          player_id: req.body.player_id,
          profile_id: req.body.profile_id,
          context: req.body.session,
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

        Game.storeSession(
          req.body.session,
          req.body.profile_id,
          req.body.game,
          level,
          function (err) {
            if (err) return send(res, 500, { status: "failed" });
            next();
          }
        );
      } else {
        log.error(
          "  [!] Fake data on level, its not opened!",
          req.body.profile_id,
          level,
          leveldata
        );
        return send(res, 500, { status: "failed" });
      }
    }
  }

  static proceedSession(req, res, next) {
    if (
      ["253.229.26.17", "88.198.56.84", "34.66.9.17"].includes(
        req.body.decrypted_token.ip
      ) === true
    )
      return send(res, 500, { status: "failed" });
    //Storing session to Redis
    Game.validateSession(
      req.body.session,
      req.body.profile_id,
      req.body.game,
      req.body.result,
      function (err, session_data) {
        if (err) {
          //Storing to clickhouse
          let event = {
            event: "accelera-api",
            page: "sessions",
            status: "validation-failed",
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            player_id: req.body.player_id,
            context: req.body.session,
            details:
              req.body.result !== undefined
                ? req.body.result.toString()
                : "undefined",
            additional: JSON.stringify(session_data),
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

          //Returning 200 to prevent fraud
          return send(res, 200, { session: req.body.session });
        } else {
          //Storing to clickhouse
          let event = {
            event: "accelera-api",
            page: "sessions",
            status: "validation-passed",
            game_id: req.body.game.game_id,
            profile_id: req.body.profile_id,
            player_id: req.body.player_id,
            context: req.body.session,
            details: req.body.result.toString(),
            additional: JSON.stringify(session_data),
            timestamp: Math.floor(new Date()),
            date: moment(new Date()).format("YYYY-MM-DD"),
            time: moment(new Date()).format("HH:mm"),
            datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          };

          log.info(
            "Got level counter (can be undefined if new level):",
            req.body.counters["level_" + session_data.level],
            session_data.level
          );
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

          req.body.session_data = session_data;

          next();
        }
      }
    );
  }

  static reloadBirthdayPartners(req, res, next) {
    //Getting crate rewards
    redis.hget(
      "platform:birthday:partners",
      req.body.profile_id,
      function (err, result) {
        if (result !== null) {
          next();
        } else {
          crate.getBirthdayRewardsByCTN(
            req.body.player_id.substring(1, 11),
            function (err, metkauser) {
              log.info(
                "Got BIRTHDAY user from crate.io:",
                req.body.player_id.substring(1, 11),
                metkauser
              );
              if (metkauser !== undefined && !err) {
                let treasure_partner = [];
                for (let i in metkauser) {
                  if (
                    metkauser[i] !== req.body.player_id.substring(1, 11) &&
                    metkauser[i] !== ""
                  )
                    treasure_partner.push(metkauser[i]);
                }

                redis.hset(
                  "platform:birthday:partners",
                  req.body.profile_id,
                  JSON.stringify(treasure_partner),
                  function (err, done) {
                    log.warn(
                      "Birthday partners are reloaded to redis",
                      req.body.profile_id,
                      JSON.stringify(treasure_partner)
                    );
                    next();
                  }
                );
              } else {
                redis.hset(
                  "platform:birthday:partners",
                  req.body.profile_id,
                  JSON.stringify([]),
                  function (err, done) {
                    next();
                  }
                );
              }
            }
          );
        }
      }
    );
  }

  static getAdditionalRewardsFromCrate(req, res, next) {
    redis.hget(
      "platform:tabby:additional-promotion",
      req.body.profile_id,
      function (err, result) {
        if (result !== null) {
          next();
        } else {
          //Select first time
          crate.getAdditionalRewardsByCTN(
            req.body.player_id.substring(1, 11),
            function (err, metkauser) {
              log.warn(
                "Got additional rewards from crate.io:",
                req.body.player_id.substring(1, 11),
                metkauser
              );
              if (metkauser !== undefined && !err) {
                let treasure_partner = [];
                for (let i in metkauser) {
                  if (
                    metkauser[i] !== req.body.player_id.substring(1, 11) &&
                    metkauser[i] !== ""
                  )
                    treasure_partner.push(metkauser[i]);
                }

                //Storing treasure partners
                redis.hset(
                  "platform:tabby:additional-promotion",
                  req.body.profile_id,
                  JSON.stringify(treasure_partner),
                  function (err, done) {
                    log.info(
                      "Additional partners are stored to redis",
                      req.body.profile_id,
                      JSON.stringify(treasure_partner)
                    );
                    next();
                  }
                );
              } else {
                redis.hset(
                  "platform:tabby:additional-promotion",
                  req.body.profile_id,
                  JSON.stringify([]),
                  function (err, done) {
                    next();
                  }
                );
              }
            }
          );
        }
      }
    );
  }

  static reloadtabbyRewardsFromCrate(req, res, next) {
    redis.hget(
      "platform:tabby:partners",
      req.body.profile_id,
      function (err, result) {
        if (result !== null) {
          next();
        } else {
          //Select first time
          crate.getRewardsByCTN(
            req.body.player_id.substring(1, 11),
            function (err, metkauser) {
              log.info(
                "Reloading tabby rewards from crate.io:",
                req.body.player_id.substring(1, 11),
                metkauser
              );
              if (metkauser !== undefined && !err) {
                let treasure_partner = [];
                for (let i in metkauser) {
                  if (
                    metkauser[i] !== req.body.player_id.substring(1, 11) &&
                    metkauser[i] !== ""
                  )
                    treasure_partner.push(metkauser[i]);
                }

                //Storing treasure partners
                redis.hset(
                  "platform:tabby:partners",
                  req.body.profile_id,
                  JSON.stringify(treasure_partner),
                  function (err, done) {
                    log.info(
                      "tabby partners are reloaded to redis",
                      req.body.profile_id,
                      JSON.stringify(treasure_partner)
                    );
                    next();
                  }
                );
              } else {
                redis.hset(
                  "platform:tabby:partners",
                  req.body.profile_id,
                  JSON.stringify([]),
                  function (err, done) {
                    next();
                  }
                );
              }
            }
          );
        }
      }
    );
  }

  static getMapSettingsbyProfile(req, res, next) {
    log.debug("Getting optin map:", req.body.game_id, req.body.profile_id);

    if (
      ["253.229.26.17", "88.198.56.84", "34.66.9.17"].includes(
        req.body.decrypted_token.ip
      ) === true
    )
      return send(res, 500, { status: "failed" });

    //TODO: important replace
    const replace_map = [];
    // [{
    //                 "replace" : "x-41",
    //                 "to" : "0"
    //             },
    //             {
    //                 "replace" : "x-54",
    //                 "to" : "0"
    //             }]

    //New one: check map version and regenerate it
    if (req.body.counters !== undefined) {
      if (req.body.counters.map_version !== req.body.game.map_version) {
        log.warn(
          "Regenerating a map to:",
          req.body.game.map_version,
          req.body.profile_id
        );
        Profiles.crateCreatetabbyMap(req, [], function (err, created_map) {
          //Storing new map
          let player = {
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            name: "map_version",
            value: req.body.game.map_version,
          };
          Counters.create({ body: player }, function (err, updates) {});

          redis.hset(
            "platform:tabby:optins",
            req.body.profile_id,
            JSON.stringify(created_map),
            function (err, done) {
              req.body.levels = created_map;

              async.map(replace_map, replaceMap, function (err) {
                if (err) {
                  log.error("Gor error while replacing a map:", err);
                } else {
                  log.info("Replacement is done");
                  next();
                }
              });
            }
          );
        });
      } else {
        redis.hget(
          "platform:tabby:optins",
          req.body.profile_id,
          function (err, levels) {
            if (err) {
              log.error("Failed to get levels from redis:", err);
              req.body.levels = [];
              next();
            } else {
              req.body.levels = JSON.parse(levels);

              async.map(replace_map, replaceMap, function (err) {
                if (err) {
                  log.error("Gor error while replacing a map:", err);
                } else {
                  log.info("Replacement is done");
                  next();
                }
              });
            }
          }
        );
      }
    } else {
      redis.hget(
        "platform:tabby:optins",
        req.body.profile_id,
        function (err, levels) {
          if (err) {
            log.error("Failed to get levels from redis:", err);
            req.body.levels = [];
            next();
          } else {
            req.body.levels = JSON.parse(levels);

            async.map(replace_map, replaceMap, function (err) {
              if (err) {
                log.error("Gor error while replacing a map:", err);
              } else {
                log.info("Replacement is done");
                next();
              }
            });
          }
        }
      );
    }

    function replaceMap(reward, callback) {
      for (let i in req.body.levels) {
        if (req.body.levels[i].COUNTERVALUE === reward.replace) {
          req.body.levels[i].COUNTERVALUE = reward.to;
        }
      }

      callback(null, "ok");
    }
  }

  static reloadXMAS2023RewardsFromCrate(req, res, next) {
    redis.hget(
      "platform:xmas-2023:partners",
      req.body.profile_id,
      function (err, result) {
        if (result !== null) {
          next();
        } else {
          //Select first time
          crate.getXMAS2023RewardsByCTN(
            req.body.player_id.substring(1, 11),
            function (err, metkauser) {
              log.warn(
                "Reloading xmas partners from crate.io:",
                req.body.player_id.substring(1, 11),
                metkauser
              );
              if (metkauser !== undefined && !err) {
                let treasure_partner = [];
                for (let i in metkauser) {
                  if (
                    metkauser[i] !== req.body.player_id.substring(1, 11) &&
                    metkauser[i] !== ""
                  )
                    treasure_partner.push(metkauser[i]);
                }

                //Storing treasure partners
                redis.hset(
                  "platform:xmas-2023:partners",
                  req.body.profile_id,
                  JSON.stringify(treasure_partner),
                  function (err, done) {
                    log.info(
                      "XMAS2023 partners are reloaded to redis",
                      req.body.profile_id,
                      JSON.stringify(treasure_partner)
                    );
                    next();
                  }
                );
              } else {
                redis.hset(
                  "platform:xmas-2023:partners",
                  req.body.profile_id,
                  JSON.stringify([]),
                  function (err, done) {
                    next();
                  }
                );
              }
            }
          );
        }
      }
    );
  }

  static reloadXMAS2023SegmentsFromCrate(req, res, next) {
    redis.hget(
      "platform:xmas-2023:segments",
      req.body.profile_id,
      function (err, result) {
        if (result !== null) {
          next();
        } else {
          //Select first time
          crate.getXMAS2023SegmentByCTN(
            req.body.player_id.substring(1, 11),
            function (err, segment) {
              log.warn(
                "Reloading xmas segment from crate.io:",
                req.body.player_id.substring(1, 11),
                segment
              );
              if (segment !== undefined && !err) {
                //Storing segment
                redis.hset(
                  "platform:xmas-2023:segments",
                  req.body.profile_id,
                  segment.segment,
                  function (err, done) {
                    log.warn(
                      "XMAS2023 segment is reloaded to redis",
                      req.body.profile_id,
                      segment
                    );

                    //Pushing to accelera
                    accelera
                      .publishTrigger(req.body.profile_id, "xmas2023-segment", {
                        game_id: req.body.game.game_id,
                        profile_id: req.body.profile_id,
                        player_id: req.body.player_id,
                        segment: segment.segment,
                      })
                      .then(function () {
                        log.info(
                          "Trigger was published:",
                          "xmas2023-segment",
                          req.body.profile_id
                        );
                      })
                      .catch((e) => {
                        log.error("Failed to publish trigger:", e);
                      });

                    next();
                  }
                );
              } else {
                redis.hset(
                  "platform:xmas-2023:segments",
                  req.body.profile_id,
                  "S4",
                  function (err, done) {
                    //Pushing to accelera
                    accelera
                      .publishTrigger(req.body.profile_id, "xmas2023-segment", {
                        game_id: req.body.game.game_id,
                        profile_id: req.body.profile_id,
                        player_id: req.body.player_id,
                        segment: "S4",
                      })
                      .then(function () {
                        log.info(
                          "Trigger was published:",
                          "xmas2023-segment",
                          req.body.profile_id
                        );
                      })
                      .catch((e) => {
                        log.error("Failed to publish trigger:", e);
                      });

                    next();
                  }
                );
              }
            }
          );
        }
      }
    );
  }
}

module.exports = API;
