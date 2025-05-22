let log = require("../services/bunyan").log;
const redis = require("../services/redis").redisclient;
const aes = require("../services/aes");
const md5 = require("../services/md5");
let _ = require("lodash");
const counters = "platform:profile:";
const profiles = "platform:games:";
const moment = require("moment");
const timeZone = require("moment-timezone");
const Bulk = require("./bulk");
const Leaderboard = require("./leaderboard");
const nanoid = require("../services/nanoid");
const momentTimezone = require("moment-timezone");

class Counters {
  static find(req, callback) {
    log.info("Searching counters by profile ID:", req.user.id);
    redis.hget(
      profiles + req.body.game_id + ":profiles",
      md5.md5(req.user.id),
      function (err, profile) {
        if (err || profile === null) {
          log.error("Profile not found for the user:", req.user.id);
          return callback(true, {});
        } else {
          redis.hgetall(
            counters + profile + ":counters",
            function (err, result) {
              if (err || result === null) {
                log.info(
                  "There are no counters found for:",
                  req.user.id,
                  profile
                );
                return callback(false, {});
              } else {
                log.info("Counters found:", _.size(result));
                if (_.size(result) !== 0) {
                  let i = 0;
                  let data = {};
                  _.forEach(result, function (value, key) {
                    _.set(data, key, value);
                    i++;
                  });

                  if (i === _.size(result)) {
                    return callback(null, data);
                  }
                }
              }
            }
          );
        }
      }
    );
  }

  static findbyprofile(req, callback) {
    log.info("Searching counters by profile ID:", req.body.player_id);
    redis.hgetall(
      counters + req.body.player_id + ":counters",
      function (err, result) {
        if (err || result === null) {
          log.info("There are no counters found for:", req.body.player_id);
          return callback(false, {});
        } else {
          log.info("Counters found:", _.size(result));
          if (_.size(result) !== 0) {
            let i = 0;
            let data = {};
            _.forEach(result, function (value, key) {
              _.set(data, key, value);
              i++;
            });

            if (i === _.size(result)) {
              return callback(null, data);
            }
          }
        }
      }
    );
  }

  static create(req, callback) {
    redis
      .multi()
      .hset(
        counters + req.body.player_id + ":counters",
        req.body.name,
        req.body.value
      )
      .exec(function (err, result) {
        if (err) {
          log.error(
            "Counter cannot be stored:",
            err,
            req.body.player_id,
            req.body
          );
          return callback(true);
        } else {
          log.info(
            "Counter is created:",
            req.body.name,
            req.body.value,
            result[0]
          );

          let data = {
            timestamp: Math.floor(new Date()),
            event: "accelera-api",
            page: "counters",
            profile_id: req.body.player_id,
            status: "created",
            game_id: req.body.game_id === undefined ? "" : req.body.game_id,
            details: req.body.name.toString(),
            gifts: [req.body.value.toString(), result[0].toString()],
            date: moment(new Date()).format("YYYY-MM-DD"),
            time: moment(new Date()).format("HH:mm"),
            datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          };

          Bulk.store(req.body.game_id, data, function () {});

          let counter = {};
          _.set(counter, req.body.name, req.body.value);
          return callback(false, counter);
        }
      });
  }

  static remove(req, callback) {
    log.info("Searching counters by profile ID:", req.body.profile_id);
    redis
      .multi()
      .hdel(counters + req.body.profile_id + ":counters", req.body.name)
      .exec(function (err) {
        if (err) {
          log.error(
            "Counter cannot be deleted:",
            err,
            req.body.profile_id,
            req.body
          );
          return callback(true);
        } else {
          log.info("Counter is deleted:", req.body.name);

          let data = {
            timestamp: Math.floor(new Date()),
            profile_id: req.body.profile_id,
            status: "removed",
            game_id: req.body.game_id === undefined ? "" : req.body.game_id,
            details: req.body.name.toString(),
            date: moment(new Date()).format("YYYY-MM-DD"),
            time: moment(new Date()).format("HH:mm"),
            datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          };

          Bulk.store(req.body.game_id, data, function () {});

          return callback(false);
        }
      });
  }

  static modify(req, callback) {
    log.info("Searching counters by profile ID:", req.body.player_id);
    redis
      .multi()
      .hincrby(
        counters + req.body.player_id + ":counters",
        "attempt",
        req.body.value
      )
      .exec(function (err, result) {
        if (err) {
          log.error(
            "Counter cannot be modified:",
            err,
            req.body.player_id,
            req.body
          );

          let data = {
            timestamp: Math.floor(new Date()),
            event: "accelera-api",
            page: "counters",
            profile_id: req.body.player_id,
            status: "not-modified",
            game_id: "tabby" === undefined ? "" : "tabby",
            details: req.body.name.toString(),
            gifts: [req.body.value.toString()],
            date: moment(new Date()).format("YYYY-MM-DD"),
            time: moment(new Date()).format("HH:mm"),
            datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          };

          Bulk.store("tabby", data, function () {});

          return callback(true);
        } else {
          log.info(
            "Counter is increased:",
            req.body.player_id,
            "attempt",
            req.body.value,
            result[0]
          );

          let data = {
            timestamp: Math.floor(new Date()),
            event: "accelera-api",
            page: "counters",
            player_id: req.body.player_id,
            status: "modified",
            game_id:
              req.body.game_id === undefined ? "tabby" : req.body.game_id,
            details: "attempt".toString(),
            gifts: [req.body.value.toString(), result[0].toString()],
            date: moment(new Date()).format("YYYY-MM-DD"),
            time: moment(new Date()).format("HH:mm"),
            datetime: moment(momentTimezone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          };

          Bulk.store("tabby", data, function () {});

          let counter = {};
          _.set(counter, "attempt", result[0]);
          return callback(false, counter);
        }
      });
  }
}

function isJSONstring(value) {
  try {
    JSON.parse(value);
  } catch (e) {
    return value;
  }
  return JSON.parse(value);
}

function isJSON(json) {
  let result = _.isObject(json) === true ? JSON.stringify(json) : json;
  return result;
}

module.exports = Counters;
