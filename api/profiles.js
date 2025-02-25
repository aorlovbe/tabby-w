let log = require("../services/bunyan").log;
const redis = require("../services/redis").redisclient;
const aes = require("../services/aes");
const md5 = require("../services/md5");
const nanoid = require("../services/nanoid");
let _ = require("lodash");
const profiles = "platform:profiles";
const accounts = "platform:users";
const moment = require("moment");
const timeZone = require("moment-timezone");
const Bulk = require("./bulk");
const accelera = require("../services/producer");
const jws = require("../services/jws");
const momentTimezone = require("moment-timezone");
const crate = require("../services/crateio");
const async = require("async");
const Counter = require("./counters");
const requestIp = require("request-ip");
const send = require("@polka/send-type");

const map = [
  "treasure_partner",
  "c-24-1",
  "id_partner",
  "c-25-1",
  "c-26-1",
  "id_partner",
  "c-27-1",
  "sd-5",
  "treasure_partner",
  "id_partner",
  "c-27-2",
  "id_partner",
  "c-26-2",
  "treasure_partner",
  "id_partner",
  "c-24-2",
  "treasure_partner",
  "id_partner",
  "treasure_partner",
  "sd-5",
  "id_partner",
  "c-24-10",
  "id_partner",
  "treasure_partner",
  "c-24-1",
  "sd-5",
  "r-250",
  "id_partner",
  "treasure_partner",
  "c-27-8",
  "id_partner",
  "id_partner",
  "c-24-3",
  "c-26-3",
  "id_partner",
  "c-24-4",
  "treasure_partner",
  "r-250",
  "sd-5",
  "id_partner",
  "treasure_ratings",
  "id_partner",
  "c-27-6",
  "treasure_partner",
  "id_partner",
  "c-25-2",
  "c-24-5",
  "r-150",
  "sd-5",
  "portal",
  "treasure_ratings",
  "treasure_partner",
  "id_partner",
  "c-27-3",
  "r-50",
  "id_partner",
  "treasure_partner",
  "sd-5",
  "treasure_ratings",
  "id_partner",
  "treasure_partner",
  "c-24-1",
  "c-27-8",
  "id_partner",
  "treasure_partner",
  "r-300",
  "top-92",
  "id_partner",
  "c-25-2",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "id_partner",
  "treasure_partner",
  "c-24-6",
  "c-24-7",
  "id_partner",
  "id_partner",
  "treasure_partner",
  "c-26-4",
  "sd-5",
  "id_partner",
  "treasure_partner",
  "treasure_ratings",
  "treasure_partner",
  "id_partner",
  "c-27-4",
  "treasure_partner",
  "id_partner",
  "c-24-9",
  "sd-5",
  "treasure_partner",
  "id_partner",
  "treasure_partner",
  "r-500",
  "c-27-7",
  "id_partner",
  "c-24-2",
  "treasure_partner",
  "id_partner",
  "c-24-7",
  "id_partner",
  "c-27-8",
  "treasure_partner",
  "r-100",
  "c-24-10",
  "c-25-3",
  "id_partner",
  "treasure_partner",
  "sd-5",
  "id_partner",
  "treasure_partner",
  "c-27-4",
  "r-150",
  "id_partner",
  "portal",
  "treasure_ratings",
  "treasure_partner",
  "id_partner",
  "c-25-1",
  "treasure_partner",
  "id_partner",
  "treasure_partner",
  "r-200",
  "id_partner",
  "treasure_ratings",
  "id_partner",
  "treasure_partner",
  "c-27-5",
  "id_partner",
  "r-150",
  "id_partner",
  "treasure_partner",
  "c-24-8",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "c-24-2",
  "id_partner",
  "c-24-13",
  "id_partner",
  "treasure_ratings",
  "id_partner",
  "c-27-5",
  "treasure_partner",
  "portal",
  "r-150",
  "treasure_partner",
  "id_partner",
  "c-24-3",
  "id_partner",
  "c-25-3",
  "treasure_partner",
  "id_partner",
  "c-27-8",
  "id_partner",
  "r-150",
  "c-24-11",
  "c-26-5",
  "id_partner",
  "treasure_partner",
  "portal",
  "c-24-12",
  "treasure_partner",
  "c-24-1",
  "id_partner",
  "portal",
  "treasure_partner",
  "c-27-6",
  "portal",
  "id_partner",
  "treasure_partner",
  "c-25-2",
  "c-26-4",
  "c-25-8",
  "c-27-1",
  "id_partner",
  "c-24-9",
  "treasure_partner",
  "id_partner",
  "treasure_partner",
  "c-24-4",
  "treasure_partner",
  "id_partner",
  "r-350",
  "id_partner",
  "sd-5",
  "id_partner",
  "c-24-2",
  "treasure_partner",
  "id_partner",
  "treasure_ratings",
  "r-100",
  "portal",
  "id_partner",
  "portal",
  "id_partner",
  "treasure_partner",
  "r-250",
  "treasure_partner",
  "r-250",
  "c-27-9",
  "id_partner",
  "c-26-6",
  "treasure_partner",
  "id_partner",
  "r-250",
  "portal",
  "c-24-7",
  "id_partner",
  "portal",
  "treasure_partner",
  "id_partner",
  "treasure_ratings",
  "c-25-4",
  "sd-5",
  "treasure_ratings",
  "treasure_partner",
  "id_partner",
  "c-25-5",
  "treasure_partner",
  "treasure_partner",
  "r-300",
  "id_partner",
  "portal",
  "c-27-7",
  "id_partner",
  "r-500",
  "treasure_partner",
  "sd-5",
  "id_partner",
  "c-24-6",
  "treasure_partner",
  "c-25-6",
  "id_partner",
  "portal",
  "treasure_partner",
  "treasure_partner",
  "id_partner",
  "c-24-13",
  "id_partner",
  "id_partner",
  "r-300",
  "treasure_partner",
  "treasure_ratings",
  "id_partner",
  "treasure_partner",
  "sd-5",
  "id_partner",
  "treasure_ratings",
  "treasure_partner",
  "r-500",
  "c-27-9",
  "treasure_partner",
  "id_partner",
  "c-24-4",
  "r-300",
  "treasure_partner",
  "id_partner",
  "r-300",
  "treasure_partner",
  "c-25-2",
  "treasure_ratings",
  "c-25-7",
  "id_partner",
  "r-350",
  "c-24-1",
  "portal",
  "treasure_partner",
  "id_partner",
  "c-27-8",
  "c-25-5",
  "portal",
  "portal",
  "id_partner",
  "sd-5",
  "treasure_partner",
  "id_partner",
  "c-27-2",
  "id_partner",
  "portal",
  "treasure_partner",
  "r-350",
  "portal",
  "id_partner",
  "id_partner",
  "treasure_partner",
  "sd-5",
  "id_partner",
  "c-24-11",
  "treasure_partner",
  "id_partner",
  "sd-5",
  "portal",
  "top-39",
  "r-350",
  "treasure_partner",
  "top-41",
  "c-24-5",
  "treasure_partner",
  "id_partner",
  "top-42",
  "c-27-1",
  "top-42",
  "id_partner",
  "sd-5",
  "c-26-7",
  "top-39",
  "portal",
  "r-200",
  "top-40",
  "r-200",
  "top-41",
  "treasure_partner",
  "top-42",
  "id_partner",
  "portal",
  "r-200",
  "r-250",
  "treasure_partner",
  "id_partner",
  "c-24-7",
  "treasure_ratings",
  "id_partner",
  "portal",
  "r-200",
  "id_partner",
  "portal",
  "portal",
  "top-42",
  "treasure_ratings",
  "id_partner",
  "c-24-10",
  "treasure_ratings",
  "id_partner",
  "treasure_partner",
  "portal",
  "id_partner",
  "top-88",
  "top-43",
  "id_partner",
  "treasure_ratings",
  "treasure_partner",
  "id_partner",
  "id_partner",
  "c-24-4",
  "portal",
  "r-200",
  "portal",
  "id_partner",
  "r-200",
  "treasure_ratings",
  "id_partner",
  "portal",
  "top-44",
  "top-44",
  "treasure_partner",
  "r-200",
  "portal",
  "top-25",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "c-27-7",
  "sd-5",
  "top-87",
  "portal",
  "top-45",
  "id_partner",
  "c-24-3",
  "c-25-6",
  "id_partner",
  "c-27-9",
  "treasure_partner",
  "id_partner",
  "r-500",
  "treasure_partner",
  "id_partner",
  "top-144",
  "portal",
  "treasure_ratings",
  "id_partner",
  "treasure_ratings",
  "sd-5",
  "portal",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "c-25-7",
  "c-24-11",
  "c-26-7",
  "r-500",
  "portal",
  "treasure_ratings",
  "id_partner",
  "r-500",
  "treasure_partner",
  "top-140",
  "c-24-6",
  "id_partner",
  "portal",
  "top-46",
  "treasure_ratings",
  "treasure_partner",
  "portal",
  "id_partner",
  "treasure_partner",
  "c-26-8",
  "id_partner",
  "sd-5",
  "c-25-8",
  "treasure_partner",
  "id_partner",
  "treasure_ratings",
  "id_partner",
  "c-24-12",
  "c-27-9",
  "portal",
  "portal",
  "r-500",
  "treasure_partner",
  "id_partner",
  "top-150",
  "c-26-7",
  "treasure_ratings",
  "id_partner",
  "portal",
  "treasure_partner",
  "r-50",
  "top-108",
  "top-47",
  "treasure_partner",
  "sd-5",
  "c-26-8",
  "portal",
  "c-24-2",
  "treasure_ratings",
  "r-500",
  "id_partner",
  "top-109",
  "treasure_partner",
  "id_partner",
  "c-24-13",
  "r-500",
  "r-500",
  "portal",
  "c-26-6",
  "treasure_partner",
  "id_partner",
  "top-16",
  "treasure_partner",
  "portal",
  "top-78",
  "top-15",
  "portal",
  "top-48",
  "r-250",
  "c-24-8",
  "id_partner",
  "top-55",
  "top-113",
  "c-24-5",
  "treasure_partner",
  "portal",
  "r-300",
  "c-25-9",
  "id_partner",
  "c-26-3",
  "treasure_partner",
  "portal",
  "treasure_ratings",
  "c-27-1",
  "top-112",
  "id_partner",
  "top-49",
  "id_partner",
  "c-24-5",
  "treasure_partner",
  "r-300",
  "portal",
  "id_partner",
  "portal",
  "c-27-2",
  "sd-5",
  "treasure_partner",
  "c-26-4",
  "treasure_partner",
  "r-350",
  "id_partner",
  "treasure_partner",
  "id_partner",
  "c-27-3",
  "top-23",
  "id_partner",
  "c-24-14",
  "portal",
  "top-1",
  "portal",
  "id_partner",
  "treasure_partner",
  "c-24-14",
  "portal",
  "top-69",
  "c-26-9",
  "top-2",
  "portal",
  "portal",
  "id_partner",
  "top-79",
  "id_partner",
  "top-70",
  "treasure_partner",
  "top-116",
  "id_partner",
  "portal",
  "portal",
  "top-117",
  "id_partner",
  "portal",
  "top-3",
  "portal",
  "top-109",
  "id_partner",
  "c-27-4",
  "portal",
  "portal",
  "c-27-5",
  "c-27-6",
  "portal",
  "portal",
  "c-25-9",
  "c-25-1",
  "c-26-9",
  "portal",
  "top-110",
  "id_partner",
  "portal",
  "portal",
  "portal",
  "c-25-10",
  "c-25-8",
  "portal",
  "top-123",
  "id_partner",
  "portal",
  "treasure_partner",
  "portal",
  "r-500",
  "portal",
  "c-27-7",
  "portal",
  "portal",
  "top-126",
  "c-24-3",
  "treasure_partner",
  "portal",
  "r-500",
  "portal",
  "treasure_partner",
  "c-27-8",
  "portal",
  "id_partner",
  "r-500",
  "portal",
  "treasure_ratings",
  "portal",
  "top-125",
  "top-127",
  "portal",
  "c-24-15",
  "portal",
  "c-27-9",
  "c-26-9",
  "id_partner",
  "portal",
  "treasure_ratings",
  "id_partner",
  "portal",
  "c-25-9",
  "portal",
  "c-24-10",
  "c-25-10",
  "c-26-10",
  "portal",
  "portal",
  "treasure_partner",
  "top-130",
  "portal",
  "portal",
  "sd-5",
  "portal",
  "id_partner",
  "portal",
  "top-132",
  "c-27-10",
  "c-24-6",
];

class Profiles {
  static save(req, callback) {
    this.find(req, function (err, profile) {
      if (err) return callback(true, err);
      if (profile === null) {
        log.info("Creating new profile for:", req.user.id);
        let ids = Profiles.getProfileId();

        let profile = {
          id: req.user.id,
          profile_id: ids.id,
          advanced_id: ids.advanced_id,
          game_id: req.body.game_id,
          username: req.user.username,
          email: req.user.email,
          name: req.user.name,
          surname: req.user.surname,
          gender: req.user.gender,
          subscription: "active",
          notifications: "active",
          activated: true,
          status: "created",
          timestamp: Math.floor(new Date()),
          update_date: moment(timeZone.tz("Europe/Moscow")._d).format(
            "YYYY-MM-DD HH:mm:ss"
          ),
          date: moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD"),
          time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
          datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
            "YYYY-MM-DD HH:mm:ss"
          ),
          password: aes.encrypt(req.user.password),
        };

        //Registration new profile
        redis
          .multi()
          .hset(
            "platform:games:" + req.body.game_id + ":profiles",
            md5.md5(profile.id),
            profile.profile_id
          )
          .hset(
            "platform:profiles",
            profile.profile_id,
            JSON.stringify(profile)
          )
          .exec(function (err) {
            if (err) {
              log.error("Can't store profile:" + err.message);
              callback(true);
            } else {
              log.info("New profile created:", JSON.stringify(profile));

              accelera
                .publishTrigger(profile.profile_id, "signup", {
                  game_id: profile.game_id,
                  email: profile.email,
                  channel:
                    req.body.channel === undefined
                      ? "mobile"
                      : req.body.channel,
                })
                .then(function () {
                  log.debug("Trigger was published:", "signup");
                })
                .catch((e) => {
                  log.error("Failed to publish trigger:", e);
                });

              callback(null, _.omit(profile, ["password"]));
            }
          });
      } else {
        redis.hget("platform:profiles", profile, (err, result) => {
          if (err) {
            log.error("Can't get profile:" + err.message);
            callback(true);
          } else {
            log.info("Got profile:", result);
            callback(null, _.omit(JSON.parse(result), ["password"]));
          }
        });
      }
    });
  }

  static crateCreatetabbyWMap(req, treasure_partner, callback) {
    let id_partner = _.filter(req.body.game.rewards, function (v) {
      return (v.type === "partners") & (v.prize_type === "map");
    });

    async.waterfall([processMap], function (err, result) {
      if (err) {
        log.error("Error while collecting a map:", err);
        callback(true);
      } else {
        log.info("Personal map is created:", result);
        callback(false, result);
      }
    });

    function processMap(done) {
      let settings = [];
      for (let i = 0; i < map.length; i++) {
        let gift = {
          COUNTERKEY: i + 1,
          COUNTERVALUE: getRewardByType(map[i]),
        };

        settings.push(gift);

        if (i === map.length - 1) {
          done(null, settings);
        }
      }
    }

    function getRewardByType(type) {
      switch (type) {
        case "id_partner": {
          //Algorythm 1
          let random = Math.floor(Math.random() * 9999) + 1; //1-10000
          log.info("Algorythm 1", random);
          let filtered = _.filter(id_partner, function (item) {
            return random >= item.from && random <= item.to;
          });

          //Проверка если статус у приза не активный то пустая клетка
          //log.warn('Filtered:',filtered,random);
          if (filtered !== undefined && filtered.length !== 0) {
            if (filtered[0].status !== "active") {
              return "0";
            } else {
              return filtered[0].id;
            }
          } else {
            return "0";
          }

          //let filtered = _.sample(id_partner);
          //return filtered.id
        }

        default: {
          return type;
        }
      }
    }
  }

  static register(req, callback) {
    this.find(req, function (err, profile) {
      if (err) return callback(true, err);
      if (profile === null) {
        log.info("Creating new profile for:", req.user.id);
        let ids = Profiles.getProfileId();

        //TODO: only for tabbyW
        if (req.body.game.game_id === "tabby_dev") {
          crate.getRewardsByCTN(
            req.user.id.substring(1, 11),
            function (err, metkauser) {
              log.info(
                "Got user from crate.io:",
                req.user.id.substring(1, 11),
                metkauser
              );
              if (metkauser !== undefined && !err) {
                //TODO: Need to create a map for client
                let treasure_partner = [];
                for (let i in metkauser) {
                  if (
                    metkauser[i] !== req.user.id.substring(1, 11) &&
                    metkauser[i] !== ""
                  )
                    treasure_partner.push(metkauser[i]);
                }
                Profiles.crateCreatetabbyWMap(
                  req,
                  treasure_partner,
                  function (err, created_map) {
                    //Storing treasure partners
                    redis.hset(
                      "platform:tabbyW:partners",
                      ids.id,
                      JSON.stringify(treasure_partner),
                      function (err, done) {
                        log.info(
                          "Partners are stored to redis",
                          ids.id,
                          JSON.stringify(treasure_partner)
                        );
                      }
                    );

                    redis.hset(
                      "platform:tabbyW:optins",
                      ids.id,
                      JSON.stringify(created_map),
                      function (err, done) {
                        let profile = {
                          id: req.user.id,
                          profile_id: ids.id,
                          player_id: req.body.player_id,
                          advanced_id: ids.advanced_id,
                          game_id: req.body.game_id,
                          nickname: "",
                          avatar: "",
                          email: req.body.email,
                          subscription: "active",
                          notifications: "active",
                          onboarding: "true",
                          token_created: Math.floor(new Date()),
                          activated: false,
                          timestamp: Math.floor(new Date()),
                          update_date: moment(
                            timeZone.tz("Europe/Moscow")._d
                          ).format("YYYY-MM-DD HH:mm:ss"),
                          date: moment(timeZone.tz("Europe/Moscow")).format(
                            "YYYY-MM-DD"
                          ),
                          time: moment(timeZone.tz("Europe/Moscow")).format(
                            "HH:mm"
                          ),
                          datetime: moment(
                            timeZone.tz("Europe/Moscow")._d
                          ).format("YYYY-MM-DD HH:mm:ss"),
                          password: aes.encrypt(req.user.password),
                        };

                        //Registration new profile
                        redis
                          .multi()
                          .hset(
                            "platform:games:" + req.body.game_id + ":profiles",
                            md5.md5(profile.id),
                            profile.profile_id
                          )
                          .hset(
                            "platform:profiles",
                            profile.profile_id,
                            JSON.stringify(profile)
                          )
                          .exec(function (err) {
                            if (err) {
                              log.error("Can't store profile:" + err.message);
                              callback(true);
                            } else {
                              log.info(
                                "New profile created:",
                                JSON.stringify(profile)
                              );
                              req.body.type = "signup";

                              //Pushing to accelera
                              accelera
                                .publishTrigger(profile.profile_id, "signup", {
                                  game_id: req.body.game.game_id,
                                  profile_id: profile.profile_id.toString(),
                                  player_id:
                                    req.body.player_id === undefined
                                      ? ""
                                      : req.body.player_id.toString(),
                                  channel:
                                    req.body.channel === undefined
                                      ? "mobile"
                                      : req.body.channel,
                                })
                                .then(function () {
                                  log.debug("Trigger was published:", "signup");
                                })
                                .catch((e) => {
                                  log.error("Failed to publish trigger:", e);
                                });

                              callback(null, _.omit(profile, ["password"]));
                            }
                          });
                      }
                    );
                  }
                );
              } else {
                log.warn(
                  "User was not found in crate.io, default gifts will be crated",
                  req.user.id.substring(1, 11),
                  err
                );

                //TODO: Need to create a map for nonclient

                Profiles.crateCreatetabbyWMap(
                  req,
                  [],
                  function (err, created_map) {
                    redis.hset(
                      "platform:tabby_dev:optins",
                      ids.id,
                      JSON.stringify(created_map),
                      function (err, done) {
                        let profile = {
                          id: req.user.id,
                          profile_id: ids.id,
                          player_id: req.body.player_id,
                          advanced_id: ids.advanced_id,
                          game_id: req.body.game_id,
                          nickname: "",
                          avatar: "",
                          email: req.body.email,
                          subscription: "active",
                          notifications: "active",
                          onboarding: "true",
                          activated: false,
                          timestamp: Math.floor(new Date()),
                          update_date: moment(
                            timeZone.tz("Europe/Moscow")._d
                          ).format("YYYY-MM-DD HH:mm:ss"),
                          date: moment(timeZone.tz("Europe/Moscow")).format(
                            "YYYY-MM-DD"
                          ),
                          time: moment(timeZone.tz("Europe/Moscow")).format(
                            "HH:mm"
                          ),
                          datetime: moment(
                            timeZone.tz("Europe/Moscow")._d
                          ).format("YYYY-MM-DD HH:mm:ss"),
                          password: aes.encrypt(req.user.password),
                        };

                        //Registration new profile
                        redis
                          .multi()
                          .hset(
                            "platform:games:" + req.body.game_id + ":profiles",
                            md5.md5(profile.id),
                            profile.profile_id
                          )
                          .hset(
                            "platform:profiles",
                            profile.profile_id,
                            JSON.stringify(profile)
                          )
                          .exec(function (err) {
                            if (err) {
                              log.error("Can't store profile:" + err.message);
                              callback(true);
                            } else {
                              log.info(
                                "New profile created:",
                                JSON.stringify(profile)
                              );
                              req.body.type = "signup";

                              //Pushing to accelera
                              accelera
                                .publishTrigger(profile.profile_id, "signup", {
                                  game_id: req.body.game.game_id,
                                  profile_id: profile.profile_id.toString(),
                                  player_id:
                                    req.body.player_id === undefined
                                      ? ""
                                      : req.body.player_id.toString(),
                                  channel:
                                    req.body.channel === undefined
                                      ? "mobile"
                                      : req.body.channel,
                                })
                                .then(function () {
                                  log.debug("Trigger was published:", "signup");
                                })
                                .catch((e) => {
                                  log.error("Failed to publish trigger:", e);
                                });

                              callback(null, _.omit(profile, ["password"]));
                            }
                          });
                      }
                    );
                  }
                );
              }
            }
          );
        } else {
          let profile = {
            id: req.user.id,
            profile_id: ids.id,
            metka: "",
            player_id: req.body.player_id,
            advanced_id: ids.advanced_id,
            game_id: req.body.game_id,
            nickname: "",
            avatar: "",
            email: req.body.email,
            subscription: "active",
            notifications: "active",
            onboarding: "true",
            activated: false,
            timestamp: Math.floor(new Date()),
            update_date: moment(timeZone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
            date: moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD"),
            time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
            datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
            password: aes.encrypt(req.user.password),
          };

          //Registration new profile
          redis
            .multi()
            .hset(
              "platform:games:" + req.body.game_id + ":profiles",
              md5.md5(profile.id),
              profile.profile_id
            )
            .hset(
              "platform:profiles",
              profile.profile_id,
              JSON.stringify(profile)
            )
            .exec(function (err) {
              if (err) {
                log.error("Can't store profile:" + err.message);
                callback(true);
              } else {
                log.info("New profile created:", JSON.stringify(profile));
                req.body.type = "signup";

                //Pushing to accelera
                accelera
                  .publishTrigger(profile.profile_id, "signup", {
                    game_id: req.body.game.game_id,
                    profile_id: profile.profile_id.toString(),
                    player_id:
                      req.body.player_id === undefined
                        ? ""
                        : req.body.player_id.toString(),
                    channel:
                      req.body.channel === undefined
                        ? "mobile"
                        : req.body.channel,
                  })
                  .then(function () {
                    log.debug("Trigger was published:", "signup");
                  })
                  .catch((e) => {
                    log.error("Failed to publish trigger:", e);
                  });

                callback(null, _.omit(profile, ["password"]));
              }
            });
        }
      } else {
        redis.hget("platform:profiles", profile, (err, result) => {
          if (err) {
            log.error("Can't get profile:" + err.message);
            callback(true);
          } else {
            log.info("Got profile:", result);
            let profile = JSON.parse(result);
            req.body.type = "signin";

            //Pushing to accelera
            accelera
              .publishTrigger(profile.profile_id, "signin", {
                game_id: req.body.game.game_id,
                profile_id: profile.profile_id.toString(),
                player_id:
                  req.body.player_id === undefined
                    ? ""
                    : req.body.player_id.toString(),
                channel:
                  req.body.channel === undefined ? "mobile" : req.body.channel,
                audience: "time-limited",
              })
              .then(function () {
                log.debug("Trigger was published:", "signin");
              })
              .catch((e) => {
                log.error("Failed to publish trigger:", e);
              });

            callback(null, _.omit(JSON.parse(result), ["password"]));
          }
        });
      }
    });
  }

  static find(req, callback) {
    log.info("Searching profile by ID:", req.user.id, md5.md5(req.user.id));
    redis.hget(
      "platform:games" + ":" + req.body.game_id + ":profiles",
      md5.md5(req.user.id),
      function (err, result) {
        if (err || result == null) {
          log.info(`Profile for ${req.user.id} not found`);
          return callback(null, null);
        } else {
          log.info("Profile is found by id:", req.user.id, result);
          return callback(null, result);
        }
      }
    );
  }

  static get(profile, callback) {
    log.info("Searching profile:", profile);
    redis.hget("platform:profiles", profile, function (err, result) {
      if (err) {
        log.error("Can't get profile:" + err.message);
        callback(true, {});
      } else {
        log.info("Got profile from redis:", result);
        if (result !== null) {
          let r = JSON.parse(result);
          log.info("Got profile:", r.id, r.profile_id);
        }
        redis.hgetall(
          "platform:profile:" + profile + ":tags",
          function (err, tags) {
            if (err || tags === null) {
              callback(null, _.omit(JSON.parse(result), ["password"]));
            } else {
              let t = {};
              Object.keys(tags).map(function (key) {
                return (t[key] = tags[key].split(","));
              });
              let profiles = _.omit(JSON.parse(result), ["password"]);
              profiles["tags"] = t;
              callback(null, profiles);
            }
          }
        );
      }
    });
  }

  static modify(req, callback) {
    log.info("Modifying profile by ID:", req.body.profile_id);
    redis.hget("platform:profiles", req.body.profile_id, (err, result) => {
      if (err) {
        log.error("Can't get profile:" + err.message);
        callback(true);
      } else {
        log.info("Got profile:", result);
        if (result !== null) {
          let profile = JSON.parse(result);

          if (_.size(req.body) !== 0) {
            let i = 0;
            _.forEach(req.body, function (value, key) {
              _.set(profile, key, value);
              i++;
            });

            if (i === _.size(req.body)) {
              redis.hset(
                "platform:profiles",
                req.body.profile_id,
                JSON.stringify(profile),
                function () {
                  log.info("Profile is updated:", JSON.stringify(profile));

                  let data = {
                    timestamp: Math.floor(new Date()),
                    profile_id: req.body.profile_id,
                    status: "modified",
                    game_id:
                      profile.game_id === undefined ? "" : profile.game_id,
                    context: JSON.stringify(profile),
                    date: moment(timeZone.tz("Europe/Moscow")).format(
                      "YYYY-MM-DD"
                    ),
                    time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
                    datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
                      "YYYY-MM-DD HH:mm:ss"
                    ),
                  };

                  Bulk.store("profiles", data, function () {});

                  return callback();
                }
              );
            }
          } else {
            log.info("Nothing to update");
            return callback();
          }
        } else {
          log.info("Nothing to update");
          return callback();
        }
      }
    });
  }

  static block(profile, callback) {
    log.info("Blocking profile by ID:", profile);
    redis.sadd("platform:blacklist", profile, (err, result) => {
      if (err) {
        log.error("Can't block profile:" + err.message);
        callback(true);
      } else {
        log.info("Profile was added to blacklist:", result);
        callback();
      }
    });
  }

  static ban(profile, callback) {
    log.info("Banning profile by ID:", profile);
    redis.sadd("platform:ban", profile, (err, result) => {
      if (err) {
        log.error("Can't ban profile:" + err.message);
        callback(true);
      } else {
        log.info("Profile was added to ban list:", result);
        callback();
      }
    });
  }

  static getNearestTop(position, callback) {
    try {
      const cuttedMap = _.slice(map, position, 600);
      const isSuperNumber = (element) => element === "top-11";
      const isTopNumber = (element) => element.includes("top-") === true;
      let superIndex = cuttedMap.findIndex(isSuperNumber) + 1;
      let topIndex = cuttedMap.findIndex(isTopNumber) + 1;
      let closest_top = map[topIndex + position - 1];
      let closest_super = map[position + superIndex - 1];

      callback(topIndex, superIndex, closest_top, closest_super);
    } catch (e) {
      callback(0, 0);
    }
  }

  static addtag(req, callback) {
    log.info("Modifying tags by tag category name:", req.body.category);

    redis.hget(
      "platform:profile:" + req.body.profile_id + ":tags",
      req.body.category,
      function (err, result) {
        if (err) {
          log.error(
            "Tags cannot be modified:",
            req.body.profile_id,
            req.body.category,
            err
          );
          return callback(true);
        } else {
          let tags = result === null || result === "" ? [] : result.split(",");
          let to_include = req.body.tags.split(",");

          if (to_include.length !== 0) {
            let new_tags = _.union(tags, to_include);

            redis.hset(
              "platform:profile:" + req.body.profile_id + ":tags",
              req.body.category,
              new_tags.join(),
              function () {
                log.info(
                  "Tags are updated:",
                  req.body.category,
                  new_tags.join()
                );

                let data = {
                  timestamp: Math.floor(new Date()),
                  profile_id: req.body.profile_id,
                  status: "modified",
                  type: req.body.category,
                  game_id:
                    req.body.game_id === undefined ? "" : req.body.game_id,
                  context: new_tags.join(),
                  date: moment(timeZone.tz("Europe/Moscow")).format(
                    "YYYY-MM-DD"
                  ),
                  time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
                  datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
                    "YYYY-MM-DD HH:mm:ss"
                  ),
                };

                Bulk.store("tags", data, function () {});

                return callback();
              }
            );
          } else {
            log.info("Nothing to update");
            return callback();
          }
        }
      }
    );
  }

  static removetag(req, callback) {
    log.info("Modifying tags by tag category name:", req.body.category);

    redis.hget(
      "platform:profile:" + req.body.profile_id + ":tags",
      req.body.category,
      function (err, result) {
        if (err) {
          log.error(
            "Tags cannot be modified:",
            req.body.profile_id,
            req.body.category,
            err
          );
          return callback(true);
        } else {
          let tags = result === null || result === "" ? [] : result.split(",");
          let to_exclude = req.body.tags.split(",");
          if (to_exclude.length !== 0) {
            let new_tags = tags.filter((el) => !to_exclude.includes(el));

            redis.hset(
              "platform:profile:" + req.body.profile_id + ":tags",
              req.body.category,
              new_tags.join(),
              function () {
                log.info(
                  "Tags are updated:",
                  req.body.category,
                  new_tags.join()
                );

                let data = {
                  timestamp: Math.floor(new Date()),
                  profile_id: req.body.profile_id,
                  status: "modified",
                  type: req.body.category,
                  game_id:
                    req.body.game_id === undefined ? "" : req.body.game_id,
                  context: new_tags.join(),
                  date: moment(timeZone.tz("Europe/Moscow")).format(
                    "YYYY-MM-DD"
                  ),
                  time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
                  datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
                    "YYYY-MM-DD HH:mm:ss"
                  ),
                };

                Bulk.store("tags", data, function () {});

                return callback();
              }
            );
          } else {
            log.info("Nothing to update");
            return callback();
          }
        }
      }
    );
  }

  static unblock(profile, callback) {
    log.info("Unblocking profile by ID:", profile);
    redis.srem("platform:blacklist", profile, (err, result) => {
      if (err) {
        log.error("Can't unblock profile:" + err.message);
        callback(true);
      } else {
        log.info("Profile was unblocked:", result);
        callback();
      }
    });
  }

  static unban(profile, callback) {
    log.info("Unbanning profile by ID:", profile);
    redis.srem("platform:ban", profile, (err, result) => {
      if (err) {
        log.error("Can't unban profile:" + err.message);
        callback(true);
      } else {
        log.info("Profile was unbanned:", result);
        callback();
      }
    });
  }

  static is_block(profile, callback) {
    log.info("Checking profile by ID to be blocked:", profile);
    redis.sismember("platform:blacklist", profile, (err, result) => {
      if (result === 1) {
        callback(true);
      } else {
        callback();
      }
    });
  }

  static is_ban(profile, callback) {
    log.info("Checking profile by ID to be banned:", profile);
    redis.sismember("platform:ban", profile, (err, result) => {
      if (result === 1) {
        log.warn("Profile is banned:", profile);
        callback(true);
      } else {
        callback();
      }
    });
  }

  static findbyuser(req, callback) {
    log.info(
      "Searching profile by user ID:",
      req.body.id,
      md5.md5(req.body.id)
    );
    if (req.body.id === undefined) return callback(null, null);

    redis.hget(
      "platform:games" + ":" + req.body.system + ":profiles",
      md5.md5(req.body.id),
      function (err, result) {
        if (err || result == null) {
          log.debug(`Profile for ${req.body.id} not found`);
          return callback(null, {});
        } else {
          log.info("Profile is found by id:", req.body.id, result);
          redis.hget(profiles, result, function (err, profile) {
            return callback(null, _.omit(JSON.parse(profile), ["password"]));
          });
        }
      }
    );
  }

  static activate(token, callback) {
    log.info("Activating user by token:", token);
    redis.hget(profiles, token.profile_id, function (err, result) {
      if (err || result == null) {
        log.warn(`Profile ${token.profile_id} not found`);
        return callback(true);
      } else {
        log.info("Profile is found, activating:", token.profile_id);
        let profile = JSON.parse(result);

        _.set(profile, "activated", true);

        accelera
          .publishTrigger(token.profile_id, "activated", {})
          .then(function () {
            log.debug("Trigger was published:", "activated");
          })
          .catch((e) => {
            log.error("Failed to publish trigger:", e);
          });

        redis.hset(
          profiles,
          token.profile_id,
          JSON.stringify(profile),
          function (err, result) {
            let updates = _.omit(profile, ["password"]);
            let jwt = jws.encrypt(updates);
            return callback(false, jwt);
          }
        );
      }
    });
  }

  static restore(req, callback) {
    log.info("Player is going to restore password:", req.body);

    if (req.body.password === undefined || req.body.email === "")
      return callback(true);

    //Publishing event to accelera
    let token = jws.encrypt({
      game_id: req.body.game_id,
      email: req.body.email,
      password: aes.encrypt(req.body.password),
    });
    log.info("JWC restore token:", token);

    callback();
  }

  static changePassword(token, callback) {
    log.info("Changing password for the user by token:", token);

    log.info("Searching profile by user ID:", token.email);
    if (token.email === undefined) return callback(null, null);

    redis.hget(accounts, md5.md5(token.email), function (err, result) {
      if (err || result == null) {
        log.info(`User ${token.email} not found:`, md5.md5(token.email));
        return callback(true);
      } else {
        log.info("User is found by account:", token.email);
        let account = JSON.parse(result);
        account.password = token.password;

        Bulk.store(
          "auth",
          {
            timestamp: Math.floor(new Date()),
            id: account.id,
            username: account.username,
            email: token.email.replace(/ /g, ""),
            name: account.name,
            surname: account.surname,
            gender: account.gender,
            social: account.social,
            status: "restored",
            date: moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD"),
            time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
            datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          },
          function () {}
        );

        redis.hset(
          accounts,
          md5.md5(token.email),
          JSON.stringify(account),
          (err) => {
            if (err) {
              log.error("Can't store user restored password:" + err.message);
            } else {
              log.info("User password was updated:", JSON.stringify(account));

              callback();
            }
          }
        );
      }
    });
  }

  static remove(req, callback) {
    redis
      .multi()
      .hdel("platform:profiles", req.body.profile_id)
      .hdel(
        "platform:games" + ":" + req.body.game_id + ":profiles",
        md5.md5(req.body.id)
      )
      .exec(function (err, results) {
        if (err) return callback(true);

        callback(false, results);
      });
  }

  static getProfileId() {
    let id = nanoid.get();
    let advanced_id =
      Math.floor(new Date()).toString() +
      Math.floor(Math.random() * 10).toString();
    log.info("Generating new profile ID:", id);
    return { id: id, advanced_id: advanced_id };
  }

  static getBirthdayCoupon(partner, callback) {
    switch (partner.activation_type) {
      //New discounted rewards
      case "discount": {
        //Getting coupon
        getCoupon(partner.promocode[0], function (err, promocode, len) {
          if (err) {
            //No code in stack or error
            log.error(
              "Error while getting coupon:",
              partner.promocode[0],
              len,
              err
            );
            delete partner["promocode"];
            callback(true, _.cloneDeep(partner), len);
          } else {
            partner.coupon = promocode;
            partner.link = partner.link.replace("{{promocode}}", promocode);
            delete partner["promocode"];
            callback(false, _.cloneDeep(partner), len);
          }
        });
        break;
      }
      case "unique": {
        //Getting coupon
        getCoupon(partner.promocode[0], function (err, promocode, len) {
          if (err) {
            //No code in stack or error
            log.error(
              "Error while getting coupon:",
              partner.promocode[0],
              len,
              err
            );
            delete partner["promocode"];
            callback(true, _.cloneDeep(partner), len);
          } else {
            partner.coupon = promocode;
            partner.link = partner.link.replace("{{promocode}}", promocode);
            delete partner["promocode"];
            callback(false, _.cloneDeep(partner), len);
          }
        });
        break;
      }

      case "mass_link": {
        partner.coupon = _.sample(partner.promocode);
        delete partner["promocode"];
        callback(false, _.cloneDeep(partner));

        break;
      }

      case "unique_link": {
        getCoupon(partner.promocode[0], function (err, promocode, len) {
          if (err) {
            //No code in stack or error
            log.error("Error while getting coupon:", partner.promocode[0], err);
            callback(true, _.cloneDeep(partner), len);
          } else {
            partner.link = partner.link.replace("{{promocode}}", promocode);
            delete partner["promocode"];

            callback(false, _.cloneDeep(partner), len);
          }
        });

        break;
      }

      case "unique_nolink": {
        //Getting coupon
        getCoupon(partner.promocode[0], function (err, promocode, len) {
          if (err) {
            //No code in stack or error
            log.error("Error while getting coupon:", partner.promocode[0], err);
            callback(true, _.cloneDeep(partner), len);
          } else {
            partner.coupon = promocode;
            delete partner["promocode"];

            callback(false, _.cloneDeep(partner), len);
          }
        });

        break;
      }

      case "mass_nolink": {
        partner.coupon = _.sample(partner.promocode);
        delete partner["promocode"];

        callback(false, _.cloneDeep(partner));

        break;
      }

      default: {
        delete partner["promocode"];
        callback(false, _.cloneDeep(partner));

        break;
      }
    }

    function getCoupon(stack, done) {
      redis.lpop("platform:coupons:" + stack, function (err, promocode) {
        if (err || promocode === null) {
          log.error("Error while getting coupon from stack:", stack, err);
          //done(false, 123);
          done(true);
        } else {
          done(false, promocode);
        }
      });
    }
  }

  static gettabbyWCoupon(partner, callback) {
    //Последняя проверка если у приза вдруг статус <> active
    partner["creation_date"] = moment(new Date()).format("DD/MM/YYYY");
    if (partner.status !== "active") {
      log.warn(
        "Gift is marked as not-active, player will see -Not coupons left-",
        partner.id,
        partner.status
      );
      delete partner["promocode"];
      callback(true, _.cloneDeep(partner), 0);
    } else {
      if (partner.id === "top-100" || partner.id === "top-101") {
        getCoupon(partner.promocode[0], function (err, promocode, len) {
          if (err) {
            //No code in stack or error
            log.error("Error while getting coupon:", partner.promocode[0], err);
            callback(true, _.cloneDeep(partner), len);
          } else {
            partner.link = partner.link.replace("{{promocode}}", promocode);
            delete partner["promocode"];

            callback(false, _.cloneDeep(partner), len);
          }
        });
      } else {
        switch (partner.activation_type) {
          //New discounted rewards
          case "discount": {
            //Getting coupon
            getCoupon(partner.promocode[0], function (err, promocode, len) {
              if (err) {
                //No code in stack or error
                log.error(
                  "Error while getting coupon:",
                  partner.promocode[0],
                  len,
                  err
                );
                delete partner["promocode"];
                callback(true, _.cloneDeep(partner), len);
              } else {
                partner.coupon = promocode;
                partner.link = partner.link.replace("{{promocode}}", promocode);
                delete partner["promocode"];
                callback(false, _.cloneDeep(partner), len);
              }
            });
            break;
          }
          case "unique": {
            //Getting coupon
            getCoupon(partner.promocode[0], function (err, promocode, len) {
              if (err) {
                //No code in stack or error
                log.error(
                  "Error while getting coupon:",
                  partner.promocode[0],
                  len,
                  err
                );
                delete partner["promocode"];
                callback(true, _.cloneDeep(partner), len);
              } else {
                partner.coupon = promocode;
                partner.link = partner.link.replace("{{promocode}}", promocode);
                delete partner["promocode"];
                callback(false, _.cloneDeep(partner), len);
              }
            });
            break;
          }

          case "mass_link": {
            partner.coupon = _.sample(partner.promocode);
            delete partner["promocode"];
            callback(false, _.cloneDeep(partner));

            break;
          }

          case "unique_link": {
            getCoupon(partner.promocode[0], function (err, promocode, len) {
              if (err) {
                //No code in stack or error
                log.error(
                  "Error while getting coupon:",
                  partner.promocode[0],
                  err
                );
                callback(true, _.cloneDeep(partner), len);
              } else {
                partner.link = partner.link.replace("{{promocode}}", promocode);
                delete partner["promocode"];

                callback(false, _.cloneDeep(partner), len);
              }
            });

            break;
          }

          case "unique_nolink": {
            //Getting coupon
            getCoupon(partner.promocode[0], function (err, promocode, len) {
              if (err) {
                //No code in stack or error
                log.error(
                  "Error while getting coupon:",
                  partner.promocode[0],
                  err
                );
                callback(true, _.cloneDeep(partner), len);
              } else {
                partner.coupon = promocode;
                delete partner["promocode"];

                callback(false, _.cloneDeep(partner), len);
              }
            });

            break;
          }

          case "mass_nolink": {
            partner.coupon = _.sample(partner.promocode);
            delete partner["promocode"];

            callback(false, _.cloneDeep(partner));

            break;
          }

          default: {
            delete partner["promocode"];
            callback(false, _.cloneDeep(partner));

            break;
          }
        }
      }
    }

    function getCoupon(stack, done) {
      redis
        .multi()
        .lpop("platform:coupons:" + stack)
        .llen("platform:coupons:" + stack)
        .exec(function (err, promocode) {
          if (err || promocode[0] === null) {
            log.error("Error while getting coupon from stack:", stack, err);
            done(true, null, promocode[1]);
          } else {
            done(false, promocode[0], promocode[1]);
          }
        });
    }
  }

  static gettabbyWTopRemain(partner, callback) {
    if (
      partner.activation_type === "unique" ||
      partner.activation_type === "discount" ||
      partner.id === "top-100" ||
      partner.id === "top-101"
    ) {
      redis.llen(
        "platform:coupons:" + partner.promocode[0],
        function (err, promocode) {
          if (err || promocode === null) {
            log.error(
              "Error while getting coupon remain from stack:",
              partner.promocode[0],
              err
            );
            callback(true, 0);
          } else {
            callback(false, promocode);
          }
        }
      );
    } else {
      callback(false, "еще есть");
    }
  }

  static gettabbyWCollectionCoupon(item, callback) {
    redis.lpop(
      "platform:coupons:promocodes-" + item,
      function (err, promocode) {
        if (err || promocode === null) {
          log.error("Error while getting coupon remain from stack:", item, err);
          callback(true, 0);
        } else {
          callback(false, promocode);
        }
      }
    );
  }

  static gettabbyWCollectionRemain(collection, callback) {
    redis.llen(
      "platform:coupons:promocodes-" + collection,
      function (err, promocode) {
        if (err || promocode === null) {
          log.error(
            "Error while getting coupon remain from stack:",
            collection,
            err
          );
          callback(true, 0);
        } else {
          callback(false, promocode);
        }
      }
    );
  }

  static gettabbyWPersonalPartners(req, res, next) {
    redis
      .multi()
      .hget("platform:tabby_dev:partners", req.body.profile_id)
      .hget("platform:tabby_dev:additional-promotion", req.body.profile_id)
      .exec(function (err, p) {
        log.info("Partners are found in redis", req.body.profile_id, p[0]);
        if (err || p[0] === null) {
          req.body.personal_partners = p[1] !== null ? JSON.parse(p[1]) : [];
          //req.body.personal_partners = [];
          next();
        } else {
          let p_0 = p[0] !== null ? JSON.parse(p[0]) : [];
          //let p_1 = (p[1] !== null) ? JSON.parse(p[1]) : [];

          //New will contain promo rewards
          //req.body.personal_partners = [...p_0, ...p_1];
          req.body.personal_partners = p_0;
          next();
        }
      });
  }

  static getBirthdayPersonalPartners(req, res, next) {
    redis.hget(
      "platform:birthday:partners",
      req.body.profile_id,
      function (err, p) {
        log.info(
          "Birthday partners are found in redis",
          req.body.profile_id,
          p
        );
        if (err || p === null) {
          req.body.personal_partners = [];
          next();
        } else {
          let p_1 = JSON.parse(p);
          req.body.personal_partners = p_1;
          next();
        }
      }
    );
  }

  static defineIsPortalActive(req, res, next) {
    if (req.body.counters.portal_opts !== undefined) {
      //Portal is active, cannot move
      return send(res, 500, {
        status: "Что-то пошло не так, повторите попытку позднее",
        modal: "end",
      });
    } else {
      next();
    }
  }

  static defineTabbyRatingBooster(req, res, next) {
    let multiply_rating_counter =
      req.body.counters["rating_booster"] === undefined
        ? 1
        : JSON.parse(req.body.counters["rating_booster"]); //its booster
    let steps_made =
      req.body.counters["steps_made"] === undefined
        ? 0
        : parseInt(req.body.counters["steps_made"]); //how much steps made today
    let today = Math.floor(new Date());

    if (multiply_rating_counter === 1) {
      //No booster was activated
      req.body.defined_rating_multiplier = 1;
      next();
    } else {
      if (multiply_rating_counter.expired_at < today) {
        delete req.body.counters["rating_booster"];
        req.body.defined_rating_multiplier = 1;

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
            next();
          }
        );
      } else {
        //Not expired by steps yet
        if (steps_made < multiply_rating_counter.steps) {
          //Can boost +1 step
          req.body.defined_rating_multiplier = multiply_rating_counter.value;

          let step = {
            profile_id: req.body.profile_id,
            game_id: req.body.game.game_id,
            name: "steps_made",
            value: 1,
          };

          Counter.modify({ body: step }, function (err, done) {
            next();
          });
        } else {
          //Cannot boost
          req.body.defined_rating_multiplier = 1;
          next();
        }
      }
    }
  }
}

module.exports = Profiles;
