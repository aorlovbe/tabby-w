let log = require("../services/bunyan").log;
const redis = require("../services/redis").redisclient;
const aes = require("../services/aes");
const md5 = require("../services/md5");
const sha = require("../services/sha");
let _ = require("lodash");
const axios = require("axios");
const leaderboard = "platform:leaderboard:";
const moment = require("moment");
const timeZone = require("moment-timezone");
const Bulk = require("./bulk");
const nanoid = require("../services/nanoid");
const Counters = require("./counters");
const Leaderboard = require("./leaderboard");
const settings = require("../settings");
const momentTimezone = require("moment-timezone");
const send = require("@polka/send-type");
const accelera = require("../services/producer");
const bulk = require("../services/bulk");

class Packs {
  static checkFreePack(req, counters, callback) {
    //Deleting private from game
    delete req.body.game["private"];
    if (req.body.game.free[0].length === 0) return callback();

    //Check if available
    let today = moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD");
    if (today !== counters.last_free_date) {
      req.body.game.free[0].free_available = true;
      req.body.game.free[0].time_to_free = "";

      //Publish freepack_available event
      accelera
        .publishTrigger(req.body.profile_id, "freepack_available", {
          profile_id: req.body.profile_id,
          game_id: req.body.game.game_id,
        })
        .then(function () {
          log.debug("Trigger was published:", "activated");
        })
        .catch((e) => {
          log.error("Failed to publish trigger:", e);
        });

      callback();
    } else {
      //Already took free today
      req.body.game.free[0].free_available = false;

      //Calculating time to free
      let timestamp = Math.floor(new Date());
      const interval_12 = 1000 * 60 * 60 * 12; // 12 hours in milliseconds
      const interval_24 = 1000 * 60 * 60 * 24; // 24 hours in milliseconds
      let timegmt = 3 * 60 * 60 * 1000;
      let startOfDay =
        Math.floor(timestamp / interval_24) * interval_24 - timegmt;
      let endOfDay = startOfDay + interval_24 - 1; // 23:59:59:9999
      let ms_to_freetry =
        timestamp > startOfDay + interval_12
          ? endOfDay
          : startOfDay + interval_24;

      //08:53
      req.body.game.free[0].time_to_free = convertMS(ms_to_freetry - timestamp);
      callback();
    }

    function convertMS(ms) {
      var d, h, m, s;
      s = Math.floor(ms / 1000);
      m = Math.floor(s / 60);
      s = s % 60;
      h = Math.floor(m / 60);
      m = m % 60;
      d = Math.floor(h / 24);
      h = h % 24;
      h += d * 24;
      return (h < 10 ? "0" + h : h) + ":" + (m < 10 ? "0" + m : m);
    }
  }

  static getFreePack(req, counters, callback) {
    if (req.body.game.free[0].length === 0) return callback(true);

    //Check if available
    let today = moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD");
    let multiple = 1;

    if (counters.subscription_x12 !== undefined) {
      if (counters.subscription_x12 === "activated") {
        multiple = 12;
      }
    }

    if (today !== counters.last_free_date) {
      //Updating tries count
      Counters.modify(
        {
          body: {
            game_id: req.body.game.game_id,
            profile_id: req.body.profile_id,
            name: "tries",
            value: 1 * multiple,
          },
        },
        function (err) {
          //Updating last free date
          Counters.create(
            {
              body: {
                game_id: req.body.game.game_id,
                profile_id: req.body.profile_id,
                name: "last_free_date",
                value: today,
              },
            },
            function (err, updates) {
              Counters.findbyprofile(
                {
                  body: {
                    game_id: req.body.game.game_id,
                    profile_id: req.body.profile_id,
                  },
                },
                function (err, counters) {
                  callback(false, counters);
                }
              );
            }
          );
        }
      );
    } else {
      log.error(
        "Cannot update free play counter because of date:",
        counters.last_free_date,
        today,
        req.body.profile_id
      );
      callback(true);
    }
  }

  static purchasePack(req, res, callback) {
    let pack = req.body.pack;
    //2022-04-12T09:39+03:00
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let packs = _.union(req.body.game.packs, req.body.game.special);
    let product = _.find(packs, { id: pack });

    req.body.product = product;
    let productId =
      process.env.NODE_ENV === "development"
        ? product.external_id_dev
        : product.external_id_prod;
    let phone =
      process.env.NODE_ENV === "development"
        ? 79880001893
        : parseInt(req.body.player_id);
    //sha1('999999999912022-03-28T17:16:24+03:00secretKey')

    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );
    req.body.productId = productId;
    //Getting token
    redis.hget("platform:tokens", "beeline-payments", function (err, token) {
      if (err) {
        log.error("Failed to get Beeline payment auth token:", err);
        return callback(true);
      }

      let headers =
        process.env.NODE_ENV === "development"
          ? {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
            };

      log.info("Proceed pack payment:", pack, time, productId, phone);

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/purchase",
        headers: headers,
        data: {
          phone: phone,
          productId: productId,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.debug("[info] Proceed pack payment:", response.data);

          //Increasing counters
          Counters.modify(
            {
              body: {
                game_id: req.body.game.game_id,
                profile_id: req.body.profile_id,
                name: "tries",
                value: product.rate,
              },
            },
            function (err, updates) {
              callback(false, updates);
            }
          );
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment:",
            JSON.stringify(err.response.data),
            pack,
            time,
            productId,
            phone,
            req.body.profile_id
          );

          let code =
            err.response.data.error.info !== undefined
              ? err.response.data.error.info[0].code.toString()
              : err.response.data.error.code.toString();
          //Update analytics
          let event = {
            event: "accelera-api",
            page: "packs",
            status: "purchase-failed",
            game_id: req.body.game.game_id,
            details: req.body.pack.toString(),
            gifts: [productId.toString(), code],
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

          //Increasing counters
          // Counters.modify({"body" : {
          //         "game_id" : req.body.game.game_id,
          //         "profile_id" : req.body.profile_id,
          //         "name": 'tries',
          //         "value": product.rate
          //     }}, function (err, updates) {
          //     callback(false, updates);
          // })
          // TODO: dont forget to delete
          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            switch (err.response.data.error.info[0].code) {
              case "RULE_CODE_STATUS": {
                return send(res, 400, { status: "RULE_CODE_STATUS" });
              }

              case "RULE_CODE_PAYMENT_TYPE": {
                return send(res, 400, { status: "RULE_CODE_PAYMENT_TYPE" });
              }

              case "RULE_CODE_REGION": {
                return send(res, 400, { status: "RULE_CODE_REGION" });
              }

              case "RULE_CODE_ACCOUNT": {
                return send(res, 400, { status: "RULE_CODE_ACCOUNT" });
              }

              case "RULE_CODE_SOC": {
                return send(res, 400, { status: "RULE_CODE_SOC" });
              }

              case "RULE_CODE_BALANCE": {
                return send(res, 400, { status: "RULE_CODE_BALANCE" });
              }

              default: {
                return send(res, 400, { status: "FAILED" });
              }
            }
          } catch (e) {
            return send(res, 400, { status: "FAILED" });
          }
        });
    });
  }

  static purchaseLeaderboard(req, res, callback) {
    let pack = req.body.pack;
    //2022-04-12T09:39+03:00
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let packs = _.union(
      req.body.game.packs,
      req.body.game.special,
      req.body.game.additional_packs
    );
    let product = _.find(packs, { id: pack });

    let productId =
      process.env.NODE_ENV === "development"
        ? product.external_id_dev
        : product.external_id_prod;
    let phone =
      process.env.NODE_ENV === "development"
        ? 79880001893
        : parseInt(req.body.player_id);
    //sha1('999999999912022-03-28T17:16:24+03:00secretKey')

    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );
    req.body.productId = productId;
    req.body.product = product;
    //Getting token
    redis.hget("platform:tokens", "beeline-payments", function (err, token) {
      if (err) {
        log.error("Failed to get Beeline payment auth token:", err);
        return callback(true);
      }

      let headers =
        process.env.NODE_ENV === "development"
          ? {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
            };

      log.info(
        "Proceed pack payment:",
        pack,
        time,
        productId,
        phone,
        settings.beeline.secret,
        checksum,
        token
      );

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/purchase",
        headers: headers,
        data: {
          phone: phone,
          productId: productId,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.debug("[info] Proceed pack payment:", response.data);

          //Storing points to user ID (leaderboard by user)
          req.body.name = "points";
          Leaderboard.setDaily(
            {
              body: {
                system: req.body.game_id,
                name: req.body.name,
                value: 1200,
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
                return send(res, 500, { status: "failed" });
              } else {
                log.info(
                  "Leaderboard is updated by purchasing 1000",
                  req.body.name
                );
                callback(false, {});
              }
            }
          );
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment:",
            JSON.stringify(err.response.data)
          );
          let code =
            err.response.data.error.info !== undefined
              ? err.response.data.error.info[0].code.toString()
              : err.response.data.error.code.toString();

          //Update analytics
          let event = {
            event: "accelera-api",
            page: "packs",
            status: "purchase-failed",
            game_id: req.body.game.game_id,
            details: req.body.pack.toString(),
            gifts: [productId.toString(), code],
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

          //TODO: dont forget to delete
          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            switch (err.response.data.error.info[0].code) {
              case "RULE_CODE_STATUS": {
                return send(res, 400, { status: "RULE_CODE_STATUS" });
              }

              case "RULE_CODE_PAYMENT_TYPE": {
                return send(res, 400, { status: "RULE_CODE_PAYMENT_TYPE" });
              }

              case "RULE_CODE_REGION": {
                return send(res, 400, { status: "RULE_CODE_REGION" });
              }

              case "RULE_CODE_ACCOUNT": {
                return send(res, 400, { status: "RULE_CODE_ACCOUNT" });
              }

              case "RULE_CODE_SOC": {
                return send(res, 400, { status: "RULE_CODE_SOC" });
              }

              case "RULE_CODE_BALANCE": {
                return send(res, 400, { status: "RULE_CODE_BALANCE" });
              }

              default: {
                return send(res, 400, { status: "FAILED" });
              }
            }
          } catch (e) {
            return send(res, 400, { status: "FAILED" });
          }
        });
    });
  }

  static purchaseSubscription(req, res, callback) {
    function getToken(phone, callback) {
      let headers = {
        "Content-Type": "application/json",
        "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = "cubesolutions@localhost.ru";
      let secret = "testkey";
      let url = "https://partnerka.beeline.ru/api";
      //sha1('testapp999999999912022-03-28T17:16:24+03:00secretKey').

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Authorized (api/v2) token:", response.data);
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    //let pack = req.body.pack;
    let pack = req.body.pack;
    //2022-04-12T09:39+03:00
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    //let packs = _.union(req.body.game.packs, req.body.game.special);
    //let product = _.find(packs, {id: pack});

    //req.body.product = product;
    //let productId = (process.env.NODE_ENV === 'development') ? product.external_id_dev : product.external_id_prod;
    let productId = req.body.pack;
    let phone = req.body.player_id;
    //sha1('999999999912022-03-28T17:16:24+03:00secretKey')

    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
            };

      log.info("Proceed pack payment:", pack, time, productId, phone);

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/activate",
        headers: headers,
        data: {
          phone: phone,
          productId: productId,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Subscriptions is purchased:", response.data);

          let event = {
            event: "accelera-api",
            page: "subscription",
            status: "subscription-purchased",
            game_id: req.body.game.game_id,
            details: req.body.pack.toString(),
            gifts: [productId.toString()],
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

          callback(false);
        })
        .catch((err) => {
          log.error(
            "Failed proceed subscription:",
            JSON.stringify(err.response.data),
            pack,
            time,
            productId,
            phone,
            req.body.profile_id
          );

          let code =
            err.response.data.error.info !== undefined
              ? err.response.data.error.info[0].code.toString()
              : err.response.data.error.code.toString();
          //Update analytics
          let event = {
            event: "accelera-api",
            page: "subscription",
            status: "subscription-failed",
            game_id: req.body.game.game_id,
            details: req.body.pack.toString(),
            gifts: [productId.toString(), code],
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

          //Increasing counters
          // Counters.modify({"body" : {
          //         "game_id" : req.body.game.game_id,
          //         "profile_id" : req.body.profile_id,
          //         "name": 'tries',
          //         "value": product.rate
          //     }}, function (err, updates) {
          //     callback(false, updates);
          // })

          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            switch (err.response.data.error.info[0].code) {
              case "RULE_CODE_STATUS": {
                return send(res, 400, { status: "RULE_CODE_STATUS" });
              }

              case "RULE_CODE_PAYMENT_TYPE": {
                return send(res, 400, { status: "RULE_CODE_PAYMENT_TYPE" });
              }

              case "RULE_CODE_REGION": {
                return send(res, 400, { status: "RULE_CODE_REGION" });
              }

              case "RULE_CODE_ACCOUNT": {
                return send(res, 400, { status: "RULE_CODE_ACCOUNT" });
              }

              case "RULE_CODE_SOC": {
                return send(res, 400, { status: "RULE_CODE_SOC" });
              }

              case "RULE_CODE_BALANCE": {
                return send(res, 400, { status: "RULE_CODE_BALANCE" });
              }

              default: {
                return send(res, 400, { status: "FAILED" });
              }
            }
          } catch (e) {
            return send(res, 400, { status: "FAILED" });
          }
        });
    });
  }

  static asyncGameList(req, res, done) {
    getToken(req.body.player_id, function (err, token) {
      checkGameList(token, function (err, data) {
        done();
      });
    });

    function getToken(phone, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Authorized (api/v2) token:", response.data);
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    function checkGameList(token, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let phone = parseInt(req.body.player_id);
      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      axios({
        method: "GET",
        url: url + "/v2/game/list",
        headers: headers,
        params: {
          appID: appID,
          token: token,
          phone: phone,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info(
            "[info] Games info status:",
            response.data[0].disabledDueToDebts
          );
          req.body.disabledDueToDebts =
            response.data[0].disabledDueToDebts === undefined
              ? false
              : response.data[0].disabledDueToDebts;
          callback(false);
        })
        .catch((err) => {
          log.error("Failed to get games info status:", err, url);
          callback(true);
        });
    }
  }

  static purchaseBoosterAsync(req, res, id, done) {
    let productId = id;
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let phone = parseInt(req.body.player_id);
    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );
    let appID = settings.beeline.appid;

    getToken(req.body.player_id, function (err, token) {
      let headers = {
        "Content-Type": "application/json",
      };

      log.info(
        "Proceed booster payment:",
        id,
        phone,
        settings.beeline.secret,
        checksum,
        token
      );
      let reqs = {
        phone: phone,
        productId: id,
        gameId: req.body.game.presentactivateId,
        time: time,
        signature: checksum,
      };

      axios({
        method: "POST",
        url: settings.beeline.payments + "/v2/game/purchase-async",
        params: {
          appID: appID,
          token: token,
        },
        headers: headers,
        data: reqs,
        timeout: 30000,
      })
        .then((response) => {
          log.debug("[info] Payment is requested async:", response.data);

          setTimeout(function () {
            checkStatus(token, response.data.purchaseId, function (err, data) {
              done();
            });
          }, 10000);
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment:",
            settings.beeline.payments + "/v2/game/purchase-async",
            JSON.stringify(err.response.data),
            reqs,
            token
          );

          //Update analytics
          let event = {
            event: "accelera-api",
            page: "shop",
            status: "booster-purchase-failed",
            game_id: req.body.game.game_id,
            details: id.toString(),
            gifts: [productId.toString(), JSON.stringify(err.response.data)],
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

          //TODO: dont forget to delete
          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            log.error("Boosters error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                status: err.response.data.error.info[0].error,
                modal: "end",
                gifts: [],
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "not_enough_balance",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                    gifts: [],
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                    gifts: [],
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
              gifts: [],
            });
          }
        });
    });

    function getToken(phone, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Authorized (api/v2) token:", response.data);
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    function checkStatus(token, purchaseId, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      axios({
        method: "GET",
        url: url + "/v2/game/purchase-info",
        headers: headers,
        params: {
          appID: appID,
          token: token,
          purchaseId: purchaseId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Purchase info status:", response.data);
          if (response.data.status === "error") {
            switch (response.data.error.info[0]) {
              case "RULE_CODE_STATUS": {
                return send(res, 200, {
                  status: "К сожалению, покупки вам недоступны",
                  modal: "end",
                  gifts: [],
                });
                break;
              }

              case "RULE_CODE_PAYMENT_TYPE": {
                return send(res, 200, {
                  status: "Модель оплаты не соответствует требованиям услуги",
                  modal: "end",
                  gifts: [],
                });
                break;
              }

              case "RULE_CODE_REGION": {
                return send(res, 200, {
                  status:
                    "Выбранная услуга не предоставляется в текущем регионе",
                  modal: "end",
                  gifts: [],
                });
                break;
              }

              case "RULE_CODE_ACCOUNT": {
                return send(res, 200, {
                  status: "Подключен лицевой счет",
                  modal: "end",
                  gifts: [],
                });
                break;
              }

              case "RULE_CODE_SOC": {
                return send(res, 200, {
                  status: "Стоит запрет на подключение платных услуг",
                  modal: "end",
                  gifts: [],
                });
                break;
              }

              case "RULE_CODE_BALANCE": {
                //тут я в редис сохраню
                redis
                  .multi()
                  .set(
                    "platform:payments:pending-credits:" + req.body.profile_id,
                    req.body.id
                  )
                  .expire(
                    "platform:payments:pending-credits:" + req.body.profile_id,
                    60
                  ) //1 минута
                  .exec(function (err) {
                    if (err) {
                      log.error(
                        "Credit payment",
                        req.body.game.game_id,
                        "is not created for",
                        req.body.profile_id
                      );
                    } else {
                      log.info(
                        "Credit payment is created:",
                        req.body.profile_id,
                        req.body.id
                      );
                      return send(res, 200, {
                        status: "not_enough_balance",
                        modal: "end",
                        gifts: [],
                      });
                    }
                  });

                break;
              }

              case "RULE_CODE_DUPLICATE": {
                return send(res, 200, {
                  status:
                    "Подключение невозможно, так как имеется действующая подписка в сервисе",
                  modal: "end",
                  gifts: [],
                });
                break;
              }

              default: {
                return send(res, 200, {
                  status: "Что-то пошло не так, повторите попытку позднее",
                  modal: "end",
                  gifts: [],
                });
                break;
              }
            }
          } else if (response.data.status === "in_progress") {
            log.warn("[info] Payment still in progress..");
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
              gifts: [],
            });
          } else {
            callback(false, response.data.token);
          }
        })
        .catch((err) => {
          log.error("Failed to get purchase status:", err, url);
          callback(true);
        });
    }
  }

  static purchaseBoosterAsyncConfirmCredit(req, res, id, callback) {
    let productId = id;
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let phone = parseInt(req.body.player_id);
    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );
    let appID = settings.beeline.appid;

    getToken(req.body.player_id, function (err, token) {
      let headers = {
        "Content-Type": "application/json",
      };

      log.info(
        "Proceed booster payment:",
        id,
        phone,
        settings.beeline.secret,
        checksum,
        token
      );
      let reqs = {
        phone: phone,
        productId: id,
        gameId: req.body.game.presentactivateId,
        inCredit: true,
        time: time,
        signature: checksum,
      };

      axios({
        method: "POST",
        url: settings.beeline.payments + "/v2/game/purchase-async",
        params: {
          appID: appID,
          token: token,
        },
        headers: headers,
        data: reqs,
        timeout: 30000,
      })
        .then((response) => {
          log.debug("[info] Payment is completed async:", response.data);

          setTimeout(function () {
            checkStatus(token, response.data.purchaseId, function (err, data) {
              callback(false);
            });
          }, 10000);
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment:",
            settings.beeline.payments + "/v2/game/purchase-async",
            JSON.stringify(err.response.data),
            reqs,
            token
          );

          //Update analytics
          let event = {
            event: "accelera-api",
            page: "shop",
            status: "booster-purchase-failed",
            game_id: req.body.game.game_id,
            details: id.toString(),
            gifts: [productId.toString(), JSON.stringify(err.response.data)],
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

          //TODO: dont forget to delete
          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе

          try {
            log.error("Boosters error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                status: err.response.data.error.info[0].error,
                modal: "end",
                gifts: [],
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "not_enough_balance",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                    gifts: [],
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                    gifts: [],
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
              gifts: [],
            });
          }
        });
    });

    function getToken(phone, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Authorized (api/v2) token:", response.data);
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    function checkStatus(token, purchaseId, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      axios({
        method: "GET",
        url: url + "/v2/game/purchase-info",
        headers: headers,
        params: {
          appID: appID,
          token: token,
          purchaseId: purchaseId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Purchase info status:", response.data);
          if (response.data.status === "error") {
            if (response.data.error.info === null) {
              return send(res, 200, {
                status: "not_enough_balance",
                modal: "end",
                gifts: [],
              });
            } else {
              switch (response.data.error.info[0]) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "not_enough_balance",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                    gifts: [],
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                    gifts: [],
                  });
                }
              }
            }
          } else if (response.data.status === "in_progress") {
            log.warn("[info] Payment still in progress..");
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
              gifts: [],
            });
          } else {
            callback(false, response.data.token);
          }
        })
        .catch((err) => {
          log.error("Failed to get purchase status:", err, url);
          callback(true);
        });
    }
  }

  static payCreditdebts(req, res, callback) {
    let phone = parseInt(req.body.player_id);
    let appID = settings.beeline.appid;

    getToken(req.body.player_id, function (err, token) {
      let headers = {
        "Content-Type": "application/json",
      };

      log.info(
        "Paying for credit debts:",
        phone,
        settings.beeline.secret,
        token
      );
      let reqs = {
        phone: phone,
      };

      axios({
        method: "POST",
        url: settings.beeline.payments + "/v2/game/pay-credit-debts",
        params: {
          appID: appID,
          token: token,
        },
        headers: headers,
        data: reqs,
        timeout: 30000,
      })
        .then((response) => {
          log.debug(
            "[info] Payment for credit debts is completed async:",
            response.data
          );

          setTimeout(function () {
            checkStatus(token, response.data.requestId, function (err, data) {
              return send(res, 200, {
                status: "ok",
              });
            });
          }, 2000);
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment for credit debts:",
            settings.beeline.payments + "/v2/game/pay-credit-debts",
            JSON.stringify(err.response.data),
            reqs,
            token
          );

          //Update analytics
          let event = {
            event: "accelera-api",
            page: "shop",
            status: "credit-debts-failed",
            game_id: req.body.game.game_id,
            gifts: [JSON.stringify(err.response.data)],
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

          return send(res, 200, {
            status: "not_enough_balance",
            text: "Что-то пошло не так, повторите попытку позднее",
          });
        });
    });

    function getToken(phone, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Authorized (api/v2) token:", response.data);
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    function checkStatus(token, requestId, callback) {
      let headers = {
        "Content-Type": "application/json",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      axios({
        method: "GET",
        url: url + "/v2/game/pay-credit-status",
        headers: headers,
        params: {
          appID: appID,
          token: token,
          requestId: requestId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Credit payment info status:", response.data);
          if (response.data.status === "error") {
            switch (response.data.error.info[0]) {
              case "RULE_CODE_BALANCE": {
                return send(res, 200, {
                  status: "not_enough_balance",
                  text: "Вы оплачивали покупки в игре [b]Обещанным платежом[/b] (максимальная сумма платежа может составлять 250 руб.)\n\nПожалуйста, пополните баланс основного счета и погасите задолженность, чтобы продолжать.",
                  gifts: [],
                });
              }

              default: {
                return send(res, 200, {
                  status: "not_enough_balance",
                  text: "Что-то пошло не так, повторите попытку позднее",
                });
              }
            }
          } else {
            return send(res, 200, {
              status: "not_enough_balance",
              text: "Что-то пошло не так, повторите попытку позднее",
            });
          }
        })
        .catch((err) => {
          log.error("Failed to get purchase status:", err, url);
          callback(true);
        });
    }
  }

  static purchaseBooster(req, res, id, callback) {
    //2022-04-12T09:39+03:00
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = id;
    //let phone = (process.env.NODE_ENV === 'development') ? 79880001893 : parseInt(req.body.player_id);
    let phone = parseInt(req.body.player_id);
    //sha1('999999999912022-03-28T17:16:24+03:00secretKey')

    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );

    //Getting token
    redis.hget("platform:tokens", "beeline-payments", function (err, token) {
      if (err) {
        log.error("Failed to get Beeline payment auth token:", err);
        return callback(true);
      }

      /*            let headers = (process.env.NODE_ENV === 'development') ? {
                'Authorization' : 'Bearer '+ token,
                'Content-Type' : 'application/json',
                'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            } : {
                'Authorization' : 'Bearer '+ token,
                'Content-Type' : 'application/json'
            };*/

      let headers = {
        Authorization: "Bearer " + token,
        "Content-Type": "application/json",
      };

      log.info(
        "Proceed booster payment:",
        id,
        phone,
        settings.beeline.secret,
        checksum,
        token
      );
      let reqs = {
        phone: phone,
        productId: id,
        time: time,
        signature: checksum,
      };

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/purchase",
        headers: headers,
        data: reqs,
        timeout: 30000,
      })
        .then((response) => {
          log.debug("[info] Proceed booster payment:", response.data);
          callback(false);
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment:",
            JSON.stringify(err.response.data),
            reqs,
            token
          );

          //Update analytics
          let event = {
            event: "accelera-api",
            page: "shop",
            status: "booster-purchase-failed",
            game_id: req.body.game.game_id,
            details: id.toString(),
            gifts: [productId.toString(), JSON.stringify(err.response.data)],
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

          //TODO: dont forget to delete
          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            log.error("Boosters error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                status: err.response.data.error.info[0].error,
                modal: "end",
                gifts: [],
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "not_enough_balance",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                    gifts: [],
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                    gifts: [],
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
              gifts: [],
            });
          }
        });
    });
  }

  static purchaseBirthdayTries(req, res, id, callback) {
    //2022-04-12T09:39+03:00
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = id;
    //let phone = (process.env.NODE_ENV === 'development') ? 79880001893 : parseInt(req.body.player_id);
    let phone = parseInt(req.body.player_id);
    //sha1('999999999912022-03-28T17:16:24+03:00secretKey')

    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );

    //Getting token
    redis.hget("platform:tokens", "beeline-payments", function (err, token) {
      if (err) {
        log.error("Failed to get Beeline payment auth token:", err);
        return callback(true);
      }

      /*            let headers = (process.env.NODE_ENV === 'development') ? {
                            'Authorization' : 'Bearer '+ token,
                            'Content-Type' : 'application/json',
                            'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
                        } : {
                            'Authorization' : 'Bearer '+ token,
                            'Content-Type' : 'application/json'
                        };*/

      let headers = {
        Authorization: "Bearer " + token,
        "Content-Type": "application/json",
      };

      log.info(
        "Proceed booster payment:",
        id,
        phone,
        settings.beeline.secret,
        checksum,
        token
      );
      let reqs = {
        phone: phone,
        productId: id,
        time: time,
        signature: checksum,
      };

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/purchase",
        headers: headers,
        data: reqs,
        timeout: 30000,
      })
        .then((response) => {
          log.debug("[info] Proceed booster payment:", response.data);
          callback(false);
        })
        .catch((err) => {
          log.error(
            "Failed proceed payment:",
            JSON.stringify(err.response.data),
            reqs,
            token
          );

          //Update analytics
          let event = {
            event: "accelera-api",
            page: "shop",
            status: "booster-purchase-failed",
            game_id: req.body.game.game_id,
            details: id.toString(),
            gifts: [productId.toString(), JSON.stringify(err.response.data)],
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

          //TODO: dont forget to delete
          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            log.error("Boosters error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                status: err.response.data.error.info[0].error,
                modal: "end",
                gifts: [],
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "Недостаточно средств на балансе",
                    modal: "end",
                    gifts: [],
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                    gifts: [],
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                    gifts: [],
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
              gifts: [],
            });
          }
        });
    });
  }

  static deactivateSubscription(req, res, callback) {
    function getToken(phone, callback) {
      let headers = {
        "Content-Type": "application/json",
        "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
      };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = "cubesolutions@localhost.ru";
      let secret = "testkey";
      let url = "https://partnerka.beeline.ru/api";
      //sha1('testapp999999999912022-03-28T17:16:24+03:00secretKey').

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Authorized (api/v2) token:", response.data);
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    //let pack = req.body.pack;
    let pack = req.body.pack;
    //2022-04-12T09:39+03:00
    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    //let packs = _.union(req.body.game.packs, req.body.game.special);
    //let product = _.find(packs, {id: pack});

    //req.body.product = product;
    //let productId = (process.env.NODE_ENV === 'development') ? product.external_id_dev : product.external_id_prod;
    let productId = req.body.pack;
    let phone = req.body.player_id;
    //sha1('999999999912022-03-28T17:16:24+03:00secretKey')

    let checksum = sha.encrypt(
      phone.toString() + productId.toString() + time + settings.beeline.secret
    );
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              Authorization: "Bearer " + token,
              "Content-Type": "application/json",
            };

      log.info("Proceed pack payment:", pack, time, productId, phone);

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/cancel",
        headers: headers,
        data: {
          phone: phone,
          productId: productId,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Subscriptions is deactivated:", response.data);

          let event = {
            event: "accelera-api",
            page: "subscription",
            status: "subscription-canceled",
            game_id: req.body.game.game_id,
            details: req.body.pack.toString(),
            gifts: [productId.toString()],
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

          callback(false);
        })
        .catch((err) => {
          log.error(
            "Failed proceed subscription:",
            JSON.stringify(err.response.data),
            pack,
            time,
            productId,
            phone,
            req.body.profile_id
          );

          let code =
            err.response.data.error.info !== undefined
              ? err.response.data.error.info[0].code.toString()
              : err.response.data.error.code.toString();
          //Update analytics
          let event = {
            event: "accelera-api",
            page: "subscription",
            status: "subscription-cancel-failed",
            game_id: req.body.game.game_id,
            details: req.body.pack.toString(),
            gifts: [productId.toString(), code],
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

          //Increasing counters
          // Counters.modify({"body" : {
          //         "game_id" : req.body.game.game_id,
          //         "profile_id" : req.body.profile_id,
          //         "name": 'tries',
          //         "value": product.rate
          //     }}, function (err, updates) {
          //     callback(false, updates);
          // })

          // {"error":{"code":422,"text":"Ошибка подключения услуги","textCode":"ERR_RESTRICTION","info":[{"code":"RULE_CODE_STATUS","desc":"Профиль деактивирован"}]}}
          // RULE_CODE_STATUS - Профиль деактивирован
          // RULE_CODE_PAYMENT_TYPE - Модель оплаты не соответствует требованиям услуги
          // RULE_CODE_REGION - Выбранная услуга не предоставляется в текущем регионе
          // RULE_CODE_ACCOUNT - Подключен лицевой счет
          // RULE_CODE_SOC - Стоит запрет на подключение платных услуг
          // RULE_CODE_BALANCE - Недостаточно средств на балансе
          try {
            switch (err.response.data.error.info[0].code) {
              case "RULE_CODE_STATUS": {
                return send(res, 400, { status: "RULE_CODE_STATUS" });
              }

              case "RULE_CODE_PAYMENT_TYPE": {
                return send(res, 400, { status: "RULE_CODE_PAYMENT_TYPE" });
              }

              case "RULE_CODE_REGION": {
                return send(res, 400, { status: "RULE_CODE_REGION" });
              }

              case "RULE_CODE_ACCOUNT": {
                return send(res, 400, { status: "RULE_CODE_ACCOUNT" });
              }

              case "RULE_CODE_SOC": {
                return send(res, 400, { status: "RULE_CODE_SOC" });
              }

              case "RULE_CODE_BALANCE": {
                return send(res, 400, { status: "RULE_CODE_BALANCE" });
              }

              default: {
                return send(res, 400, { status: "FAILED" });
              }
            }
          } catch (e) {
            return send(res, 400, { status: "FAILED" });
          }
        });
    });
  }

  static sendServiceCode(req, res, callback) {
    req.body.phone = req.body.player_id;
    axios({
      method: "GET",
      url: "https://bmf-otpsubs.amdigital.ru/api/otp/check_availability",
      params: {
        msisdn: req.body.phone,
        service_code: req.body.service,
      },
      timeout: 10000,
    })
      .then((response) => {
        log.info(
          "[info] Proceed service check:",
          response.data,
          req.body.phone,
          req.body.service
        );
        if (response.data.status === "success") {
          if (response.data.subscription_enabled === true) {
            let id = nanoid.getmax();
            let try_enabled = response.data.try_enabled;
            //continuing
            axios({
              method: "POST",
              url: "https://bmf-otpsubs.amdigital.ru/api/otp/send_otp",
              headers: {
                "Content-Type": "application/json",
              },
              data: {
                msisdn: req.body.phone,
                service_code: req.body.service,
                tx_id: id,
              },
              timeout: 60000,
            })
              .then((response) => {
                log.debug(
                  "[info] Proceed service request:",
                  response.data,
                  req.body.phone,
                  req.body.service,
                  id
                );
                if (response.data.status === "success") {
                  if (try_enabled === "try" || try_enabled === "unknown") {
                    return send(res, 200, {
                      status: "Введите код активации из СМС",
                      modal: "code",
                      id: id,
                      try_enabled: try_enabled,
                    });
                  } else {
                    let details = _.find(req.body.game.rewards, {
                      service_code: req.body.service,
                    });
                    log.warn("Searching for trial details,", details);
                    let wording =
                      details.trial_not_available !== undefined
                        ? details.trial_not_available
                        : "Ранее Вы уже израсходовали доступный промопериод, для полноценного подключения введите код активации из СМС";
                    return send(res, 200, {
                      status: wording,
                      modal: "code",
                      id: id,
                      try_enabled: try_enabled,
                    });
                  }
                } else {
                  return send(res, 200, {
                    status: "Что-то пошло не так. Попробуйте еще раз позднее",
                    modal: "end",
                  });
                }
              })
              .catch((err) => {
                log.error(
                  "Failed proceed service request:",
                  JSON.stringify(err),
                  req.body.phone,
                  req.body.service
                );

                return send(res, 400, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              });
          } else {
            return send(res, 200, {
              status:
                'Невозможно подключить услугу. Возможно, она была подключена ранее (проверьте в разделе "Подключенные услуги" в мобильном приложении билайна) или у вас установлен запрет на подключение контентных сервисов.',
              modal: "end",
            });
          }
        } else {
          log.error(
            "Failed proceed service request:",
            response,
            req.body.phone,
            req.body.service
          );
          try {
            switch (response.data.code) {
              case 9000: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }

              case 2300: {
                return send(res, 200, {
                  status:
                    "Проверочный код из смс был введен неверно. Попробуйте еще раз",
                  modal: "code",
                });
              }

              case 2200: {
                return send(res, 200, {
                  status:
                    "Проверочный код из смс был введен неверно несколько раз. Пожалуйста, запросите новый код",
                  modal: "end",
                });
              }

              case 2100: {
                return send(res, 200, {
                  status:
                    "Истек срок ввода проверочного кода из смс. Пожалуйста, запросите новый код",
                  modal: "end",
                });
              }

              case 1100: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }

              case 1000: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }

              default: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }
            }
          } catch (e) {
            log.error(
              "Failed proceed service request:",
              e,
              req.body.phone,
              req.body.service
            );
            return send(res, 200, {
              status: "Что-то пошло не так. Попробуйте еще раз позднее",
              modal: "end",
            });
          }
        }
      })
      .catch((err) => {
        log.error(
          "Failed proceed service request:",
          JSON.stringify(err.response.data),
          req.body.phone,
          req.body.service
        );
        //Update analytics
        let event = {
          event: "accelera-api",
          page: "services",
          status: "request-code-failed",
          game_id: req.body.game.game_id,
          details: req.body.service.toString(),
          player_id: req.body.phone.toString(),
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

        try {
          switch (err.response.data.code) {
            case 9000: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }

            case 2300: {
              return send(res, 200, {
                status:
                  "Проверочный код из смс был введен неверно. Попробуйте еще раз",
                modal: "code",
              });
            }

            case 2200: {
              return send(res, 200, {
                status:
                  "Проверочный код из смс был введен неверно несколько раз. Пожалуйста, запросите новый код",
                modal: "end",
              });
            }

            case 2100: {
              return send(res, 200, {
                status:
                  "Истек срок ввода проверочного кода из смс. Пожалуйста, запросите новый код",
                modal: "end",
              });
            }

            case 1100: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }

            case 1000: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }

            default: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }
          }
        } catch (e) {
          return send(res, 200, {
            status: "Что-то пошло не так. Попробуйте еще раз позднее",
            modal: "end",
          });
        }
      });
  }

  static confirmServiceCode(req, res, callback) {
    //continuing

    req.body.phone = req.body.player_id;
    axios({
      method: "POST",
      url: "https://bmf-otpsubs.amdigital.ru/api/otp/verify_otp",
      headers: {
        "Content-Type": "application/json",
      },
      data: {
        msisdn: req.body.phone,
        service_code: req.body.service,
        tx_id: req.body.tx_id,
        otp_code: req.body.code.replace(" ", ""),
      },
      timeout: 30000,
    })
      .then((response) => {
        log.debug(
          "[info] Proceed service request:",
          req.body.code,
          response.data,
          req.body.phone,
          req.body.service
        );

        if (response.data.status === "success") {
          let event = {
            event: "accelera-api",
            page: "services",
            status: "confirmed",
            game_id: req.body.game.game_id,
            context: req.body.service.toString(),
            additional:
              req.body.metka !== undefined ? req.body.metka.toString() : "",
            profile_id:
              req.body.profile_id === undefined
                ? ""
                : req.body.profile_id.toString(),
            player_id:
              req.body.phone === undefined ? "" : req.body.phone.toString(),
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

          //Pushing to accelera
          accelera
            .publishTrigger(req.body.profile_id, "partner-activated", {
              game_id: req.body.game.game_id,
              profile_id: req.body.profile_id,
              player_id: req.body.player_id,
              service_code: req.body.service,
            })
            .then(function () {
              log.info(
                "Trigger was published:",
                "partner-activated",
                req.body.profile_id
              );
            })
            .catch((e) => {
              log.error("Failed to publish trigger:", e);
            });

          return send(res, 200, {
            status:
              "Запрос на подключение услуги принят, ждите СМС со статусом подключения!",
            modal: "end",
          });
        } else {
          try {
            let code =
              response.data.code === undefined
                ? "500"
                : response.data.code.toString();
            let re =
              response.data === undefined
                ? "none"
                : JSON.stringify(response.data);
            log.error(
              "Error while confirming service activation:",
              response.data,
              req.body.phone,
              req.body.service,
              response
            );
            let event = {
              event: "accelera-api",
              page: "services",
              status: "confirmation-failed",
              game_id: req.body.game.game_id,
              context: req.body.service.toString(),
              gifts: [code, re],
              profile_id:
                req.body.profile_id === undefined
                  ? ""
                  : req.body.profile_id.toString(),
              player_id:
                req.body.phone === undefined ? "" : req.body.phone.toString(),
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

            switch (response.data.code) {
              case 9000: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }

              case 2300: {
                return send(res, 200, {
                  status:
                    "Проверочный код из смс был введен неверно. Попробуйте еще раз",
                  modal: "code",
                });
              }

              case 2200: {
                return send(res, 200, {
                  status:
                    "Проверочный код из смс был введен неверно несколько раз. Пожалуйста, запросите новый код",
                  modal: "end",
                });
              }

              case 2100: {
                return send(res, 200, {
                  status:
                    "Истек срок ввода провероного кода из смс. Пожалуйста, запросите новый код",
                  modal: "end",
                });
              }

              case 1100: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }

              case 1000: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }

              default: {
                return send(res, 200, {
                  status: "Что-то пошло не так. Попробуйте еще раз позднее",
                  modal: "end",
                });
              }
            }
          } catch (e) {
            let event = {
              event: "accelera-api",
              page: "services",
              status: "confirmation-failed",
              game_id: req.body.game.game_id,
              context: req.body.service.toString(),
              gifts: ["500"],
              profile_id:
                req.body.profile_id === undefined
                  ? ""
                  : req.body.profile_id.toString(),
              player_id:
                req.body.phone === undefined ? "" : req.body.phone.toString(),
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

            return send(res, 200, {
              status: "Что-то пошло не так. Попробуйте еще раз позднее",
              modal: "end",
            });
          }
        }
      })
      .catch((err) => {
        log.error(
          "Failed proceed service request:",
          req.body.phone,
          req.body.service,
          req.body.code,
          req.body.tx_id,
          err
        );
        //Update analytics
        let event = {
          event: "accelera-api",
          page: "services",
          status: "confirmation-failed",
          game_id: req.body.game.game_id,
          details: req.body.service.toString(),
          profile_id:
            req.body.profile_id === undefined
              ? ""
              : req.body.profile_id.toString(),
          player_id: req.body.phone.toString(),
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

        try {
          switch (err.response.data.code) {
            case 9000: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }

            case 2300: {
              return send(res, 200, {
                status:
                  "Проверочный код из смс был введен неверно. Попробуйте еще раз",
                modal: "code",
              });
            }

            case 2200: {
              return send(res, 200, {
                status:
                  "Проверочный код из смс был введен неверно несколько раз. Пожалуйста, запросите новый код",
                modal: "end",
              });
            }

            case 2100: {
              return send(res, 200, {
                status:
                  "Истек срок ввода провероного кода из смс. Пожалуйста, запросите новый код",
                modal: "end",
              });
            }

            case 1100: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }

            case 1000: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }

            default: {
              return send(res, 200, {
                status: "Что-то пошло не так. Попробуйте еще раз позднее",
                modal: "end",
              });
            }
          }
        } catch (e) {
          return send(res, 200, {
            status: "Что-то пошло не так. Попробуйте еще раз позднее",
            modal: "end",
          });
        }
      });
  }

  //For activation_type = ingame_partner
  static sendPartnerActivationRequest(req, res, callback) {
    function getToken(phone, callback) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn(
            "[info] Authorized (partners api/v2) token:",
            checksum,
            response.data
          );
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    function checkProducts(code, token, phone, callback) {
      let url = settings.beeline.payments;

      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let appID = settings.beeline.appid;

      axios({
        method: "GET",
        url: url + "/v2/game/presents",
        headers: headers,
        params: {
          phone: phone,
          appID: appID,
          token: token,
          gameId: req.body.game.presentactivateId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Checking for a partner products:", response.data);
          //Getting this product
          let product = _.find(response.data, {
            id: parseInt(req.body.service),
          });
          callback(
            false,
            product === undefined ? true : product.trial_availability
          );
        })
        .catch((err) => {
          log.error("Failed to getting partner products:", err);
          callback(true, []);
        });
    }

    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = req.body.service;
    let phone = req.body.player_id;
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      checkProducts(
        productId,
        token,
        phone,
        function (err, trial_availability) {
          if (trial_availability === false) {
            //Trial is not available, so checking for right text
            let details = _.find(req.body.game.rewards, {
              service_code: req.body.service.toString(),
            });
            log.warn("Searching for trial details,", details);
            if (details.type === "up") {
              //Its up tariff
              redis.hget(
                "platform:xmas-2023:tariffs",
                req.body.profile_id,
                function (err, up) {
                  log.info("UP is revoked to redis:", up);
                  let tariff = JSON.parse(up);
                  let wording = `Нажмите на кнопку «Оплатить» и с вашего баланса спишется абонентская плата со скидкой [color=#9C9C9C][s]${tariff.price_before}[/s][/color] ${tariff.price_after} ₽ за месяц`;
                  return send(res, 200, {
                    topic: "Тариф UP",
                    btn_name: "Подтвердить",
                    action: "",
                    status: wording,
                    modal: "confirm",
                  });
                }
              );
            } else {
              let wording =
                details.trial_not_available !== undefined
                  ? details.trial_not_available
                  : "Ваш промопериод уже израсходован. Подтвердите для подключения.";
              return send(res, 200, {
                topic: "Активация",
                btn_name: "Подтвердить",
                action: "",
                status: wording,
                modal: "confirm",
              });
            }
          } else {
            let headers =
              process.env.NODE_ENV === "development"
                ? {
                    "Content-Type": "application/json",
                    "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
                  }
                : {
                    "Content-Type": "application/json",
                  };

            log.info("Proceed present activation:", time, productId, phone);
            let appID = settings.beeline.appid;

            axios({
              method: "POST",
              url: settings.beeline.payments + "/v2/game/present-activate",
              headers: headers,
              params: {
                appID: appID,
                token: token,
              },
              data: {
                phone: phone,
                presentId: productId,
                gameId: req.body.game.presentactivateId,
              },
              timeout: 30000,
            })
              .then((response) => {
                log.warn("[info] Present is purchased:", response.data);

                let event = {
                  event: "accelera-api",
                  page: "presents",
                  status: "present-purchased",
                  game_id: req.body.game.game_id,
                  details: req.body.productId.toString(),
                  gifts: [productId.toString()],
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

                return send(res, 200, {
                  topic: "Активация",
                  btn_name: "Отлично!",
                  action: "",
                  status:
                    "Запрос на подключение услуги принят, ждите СМС со статусом подключения!",
                  modal: "end",
                });
              })
              .catch((err) => {
                log.error(
                  "Failed proceed partners subscription:",
                  JSON.stringify(err.response.data),
                  time,
                  productId,
                  phone,
                  req.body.profile_id
                );

                let code =
                  err.response.data.error.code !== undefined
                    ? err.response.data.error.code.toString()
                    : err.response.data.error.toString();
                //Update analytics
                let event = {
                  event: "accelera-api",
                  page: "presents",
                  status: "present-failed",
                  game_id: req.body.game.game_id,
                  details: req.body.productId.toString(),
                  gifts: [productId.toString(), code],
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

                try {
                  log.error("Partners error:", err.response.data.error.code);
                  if (err.response.data.error.code === 400) {
                    return send(res, 200, {
                      topic: "Активация",
                      btn_name: "Понятно",
                      action: "",
                      status: err.response.data.error.info[0].error,
                      modal: "end",
                    });
                  } else {
                    switch (err.response.data.error.info[0].code) {
                      case "RULE_CODE_STATUS": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status: "К сожалению, покупки вам недоступны",
                          modal: "end",
                        });
                      }

                      case "RULE_CODE_PAYMENT_TYPE": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status:
                            "Модель оплаты не соответствует требованиям услуги",
                          modal: "end",
                        });
                      }

                      case "RULE_CODE_REGION": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status:
                            "Выбранная услуга не предоставляется в текущем регионе",
                          modal: "end",
                        });
                      }

                      case "RULE_CODE_ACCOUNT": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status: "Подключен лицевой счет",
                          modal: "end",
                        });
                      }

                      case "RULE_CODE_SOC": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status: "Стоит запрет на подключение платных услуг",
                          modal: "end",
                        });
                      }

                      case "RULE_CODE_BALANCE": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Пополнить баланс",
                          action: "balance",
                          status: "Недостаточно средств на балансе",
                          modal: "end",
                        });
                      }

                      case "RULE_CODE_DUPLICATE": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status:
                            "Подключение невозможно, так как имеется действующая подписка в сервисе",
                          modal: "end",
                        });
                      }

                      case "ALREADY_ACTIVATED": {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status: "Услуга уже активирована",
                          modal: "end",
                        });
                      }

                      default: {
                        return send(res, 200, {
                          topic: "Активация",
                          btn_name: "Понятно",
                          action: "",
                          status:
                            "Что-то пошло не так, повторите попытку позднее",
                          modal: "end",
                        });
                      }
                    }
                  }
                } catch (e) {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                  });
                }
              });
          }
        }
      );
    });
  }

  static sendCheckProducts(req, service_code, next) {
    function getToken(phone, callback) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone, //9031298101 для теста //phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info(
            "[info] Authorized (partners api/v2) token:",
            phone,
            checksum,
            response.data
          );
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    function checkProducts(service_code, token, phone, callback) {
      let url = settings.beeline.payments;

      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let appID = settings.beeline.appid;

      axios({
        method: "GET",
        url: url + "/v2/game/presents",
        headers: headers,
        params: {
          phone: phone, //9031298101 для теста //phone
          appID: appID,
          token: token,
          gameId: req.body.game.presentactivateId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.info("[info] Checking for a partner products:", response.data);
          //Getting this product
          let product = _.find(response.data, { id: service_code });
          callback(false, product);
        })
        .catch((err) => {
          log.error("Failed to getting partner products:", err);
          callback(true, {});
        });
    }

    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = req.body.service;
    let phone = req.body.player_id; //9031298101 req.body.player_id
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      checkProducts(service_code, token, phone, function (err, products) {
        req.body.checked_product = products;
        next(err, products);
      });
    });
  }

  static sendPartnerActivationConfirm(req, res, callback) {
    function getToken(phone, callback) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn(
            "[info] Authorized (partners api/v2) token:",
            checksum,
            response.data
          );
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = req.body.service;
    let phone = req.body.player_id;
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              "Content-Type": "application/json",
            };

      log.info("Proceed present activation:", time, productId, phone);
      let appID = settings.beeline.appid;

      axios({
        method: "POST",
        url: settings.beeline.payments + "/v2/game/present-activate",
        headers: headers,
        params: {
          appID: appID,
          token: token,
        },
        data: {
          phone: phone,
          presentId: productId,
          gameId: req.body.game.presentactivateId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Present is purchased:", response.data);

          //Storing to redis request_id
          if (response.data.requestId !== undefined) {
            //тут я в редис сохраню
            redis
              .multi()
              .set(
                "platform:payments:pending-requests:" + req.body.profile_id,
                response.data.requestId
              )
              .expire(
                "platform:payments:pending-requests:" + req.body.profile_id,
                180
              ) //2 минуты
              .exec(function (err) {
                if (err) {
                  log.error(
                    "Pending request ID:",
                    req.body.game.game_id,
                    "is not created for",
                    req.body.profile_id,
                    response.data.requestId
                  );
                } else {
                  log.info(
                    "Pending request ID:",
                    req.body.game.game_id,
                    "is created for",
                    req.body.profile_id,
                    response.data.requestId
                  );
                }
              });
          }

          let event = {
            event: "accelera-api",
            page: "presents",
            status: "present-purchased",
            game_id: req.body.game.game_id,
            details: req.body.productId.toString(),
            gifts: [productId.toString()],
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

          //Pushing to accelera
          accelera
            .publishTrigger(req.body.profile_id, "partner-activated", {
              game_id: req.body.game.game_id,
              profile_id: req.body.profile_id,
              player_id: req.body.player_id,
              service_code: productId,
            })
            .then(function () {
              log.info(
                "Trigger was published:",
                "partner-activated",
                req.body.profile_id
              );
            })
            .catch((e) => {
              log.error("Failed to publish trigger:", e);
            });

          return send(res, 200, {
            topic: "Активация",
            btn_name: "Отлично!",
            action: "",
            status:
              "Запрос на подключение услуги принят, ждите СМС со статусом подключения!",
            modal: "end",
          });
        })
        .catch((err) => {
          log.error(
            "Failed proceed partners subscription:",
            JSON.stringify(err.response.data),
            time,
            productId,
            phone,
            req.body.profile_id
          );

          let code =
            err.response.data.error.code !== undefined
              ? err.response.data.error.code.toString()
              : err.response.data.error.toString();
          //Update analytics
          let event = {
            event: "accelera-api",
            page: "presents",
            status: "present-failed",
            game_id: req.body.game.game_id,
            details: req.body.productId.toString(),
            gifts: [productId.toString(), code],
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

          try {
            log.error("Partners error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                topic: "Активация",
                btn_name: "Понятно",
                action: "",
                status: err.response.data.error.info[0].error,
                modal: "end",
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "Подключен лицевой счет",
                    modal: "end",
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Пополнить баланс",
                    action: "balance",
                    status: "Недостаточно средств на балансе",
                    modal: "end",
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                  });
                }

                case "ALREADY_ACTIVATED": {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "Услуга уже активирована",
                    modal: "end",
                  });
                }

                default: {
                  return send(res, 200, {
                    topic: "Активация",
                    btn_name: "Понятно",
                    action: "",
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              topic: "Активация",
              btn_name: "Понятно",
              action: "",
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
            });
          }
        });
    });
  }

  static sendSotActivation(req, res, callback) {
    log.info("Got SOT activation request:", req.body);
    function getToken(phone, callback) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/v2/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn(
            "[info] Authorized (partners api/v2) token:",
            checksum,
            response.data
          );
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = req.body.service;
    let phone = req.body.player_id;
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              "Content-Type": "application/json",
            };

      log.info("Proceed present activation:", time, productId, phone);
      let appID = settings.beeline.appid;

      axios({
        method: "POST",
        url: settings.beeline.payments + "/v2/game/present-activate",
        headers: headers,
        params: {
          appID: appID,
          token: token,
        },
        data: {
          phone: phone,
          presentId: parseInt(productId),
          gameId: req.body.game.presentactivateId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Present is purchased:", response.data);

          let event = {
            event: "accelera-api",
            page: "presents",
            status: "present-purchased",
            game_id: req.body.game.game_id,
            details: req.body.productId.toString(),
            gifts: [productId.toString()],
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

          //Pushing to accelera
          accelera
            .publishTrigger(req.body.profile_id, "partner-activated", {
              game_id: req.body.game.game_id,
              profile_id: req.body.profile_id,
              player_id: req.body.player_id,
              service_code: productId,
            })
            .then(function () {
              log.info(
                "Trigger was published:",
                "partner-activated",
                req.body.profile_id
              );
            })
            .catch((e) => {
              log.error("Failed to publish trigger:", e);
            });

          return send(res, 200, {
            status:
              "Запрос на подключение услуги принят, ждите СМС со статусом подключения!",
            modal: "end",
          });
        })
        .catch((err) => {
          log.error(
            "Failed proceed partners subscription:",
            JSON.stringify(err.response.data),
            time,
            productId,
            phone,
            req.body.profile_id
          );

          let code =
            err.response.data.error.code !== undefined
              ? err.response.data.error.code.toString()
              : err.response.data.error.toString();
          //Update analytics
          let event = {
            event: "accelera-api",
            page: "presents",
            status: "present-failed",
            game_id: req.body.game.game_id,
            details: req.body.productId.toString(),
            gifts: [productId.toString(), code],
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

          try {
            log.error("Partners error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                status: err.response.data.error.info[0].error,
                modal: "end",
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "Недостаточно средств на балансе",
                    modal: "end",
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
            });
          }
        });
    });
  }

  static sendPurchaseAsync(req, res, callback) {
    function getToken(phone, callback) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              //'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
            }
          : {
              "Content-Type": "application/json",
            };

      let time = moment(momentTimezone.tz("Europe/Moscow")._d)
        .subtract(3, "hour")
        .format("YYYY-MM-DDTHH:mm:ssZ");
      let appID = settings.beeline.appid;
      let secret = settings.beeline.secret;
      let url = settings.beeline.payments;

      let checksum = sha.encrypt(appID + phone.toString() + time + secret);

      axios({
        method: "POST",
        url: url + "/game/token",
        headers: headers,
        data: {
          phone: phone,
          appID: appID,
          time: time,
          signature: checksum,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn(
            "[info] Authorized (partners api/v2) token:",
            checksum,
            response.data
          );
          callback(false, response.data.token);
        })
        .catch((err) => {
          log.error("Failed to get Beeline payment auth v2 token:", err, url);
          callback(true);
        });
    }

    let time = moment(momentTimezone.tz("Europe/Moscow")._d)
      .subtract(3, "hour")
      .format("YYYY-MM-DDTHH:mm:ssZ");
    let productId = req.body.service;
    let phone = req.body.player_id;
    req.body.productId = productId;

    getToken(phone, function (err, token) {
      let headers =
        process.env.NODE_ENV === "development"
          ? {
              "Content-Type": "application/json",
              "X-REDIRECT-TO": "73045e07-65c2-4400-aba5-1e6bcb1f4f6d",
            }
          : {
              "Content-Type": "application/json",
            };

      log.info("Proceed present activation:", time, productId, phone);
      let appID = settings.beeline.appid;

      axios({
        method: "POST",
        url: settings.beeline.payments + "/game/purchase-async",
        headers: headers,
        params: {
          appID: appID,
          token: token,
        },
        data: {
          phone: phone,
          presentId: productId,
          gameId: req.body.game.presentactivateId,
        },
        timeout: 30000,
      })
        .then((response) => {
          log.warn("[info] Present is purchased:", response.data);

          let event = {
            event: "accelera-api",
            page: "presents",
            status: "present-purchased",
            game_id: req.body.game.game_id,
            details: req.body.productId.toString(),
            gifts: [productId.toString()],
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

          return send(res, 200, {
            status:
              "Запрос на подключение услуги принят, ждите СМС со статусом подключения!",
            modal: "end",
          });
        })
        .catch((err) => {
          log.error(
            "Failed proceed partners subscription:",
            JSON.stringify(err.response.data),
            time,
            productId,
            phone,
            req.body.profile_id
          );

          let code =
            err.response.data.error.code !== undefined
              ? err.response.data.error.code.toString()
              : err.response.data.error.toString();
          //Update analytics
          let event = {
            event: "accelera-api",
            page: "presents",
            status: "present-failed",
            game_id: req.body.game.game_id,
            details: req.body.productId.toString(),
            gifts: [productId.toString(), code],
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

          try {
            log.error("Partners error:", err.response.data.error.code);
            if (err.response.data.error.code === 400) {
              return send(res, 200, {
                status: err.response.data.error.info[0].error,
                modal: "end",
              });
            } else {
              switch (err.response.data.error.info[0].code) {
                case "RULE_CODE_STATUS": {
                  return send(res, 200, {
                    status: "К сожалению, покупки вам недоступны",
                    modal: "end",
                  });
                }

                case "RULE_CODE_PAYMENT_TYPE": {
                  return send(res, 200, {
                    status: "Модель оплаты не соответствует требованиям услуги",
                    modal: "end",
                  });
                }

                case "RULE_CODE_REGION": {
                  return send(res, 200, {
                    status:
                      "Выбранная услуга не предоставляется в текущем регионе",
                    modal: "end",
                  });
                }

                case "RULE_CODE_ACCOUNT": {
                  return send(res, 200, {
                    status: "Подключен лицевой счет",
                    modal: "end",
                  });
                }

                case "RULE_CODE_SOC": {
                  return send(res, 200, {
                    status: "Стоит запрет на подключение платных услуг",
                    modal: "end",
                  });
                }

                case "RULE_CODE_BALANCE": {
                  return send(res, 200, {
                    status: "Недостаточно средств на балансе",
                    modal: "end",
                  });
                }

                case "RULE_CODE_DUPLICATE": {
                  return send(res, 200, {
                    status:
                      "Подключение невозможно, так как имеется действующая подписка в сервисе",
                    modal: "end",
                  });
                }

                default: {
                  return send(res, 200, {
                    status: "Что-то пошло не так, повторите попытку позднее",
                    modal: "end",
                  });
                }
              }
            }
          } catch (e) {
            return send(res, 200, {
              status: "Что-то пошло не так, повторите попытку позднее",
              modal: "end",
            });
          }
        });
    });
  }
}

module.exports = Packs;
