const polka = require("polka");
const router = polka();
const passport = require("../middleware/passport-auth");
const log = require("../services/bunyan").log;
const redis = require("../services/redis").redisclient;
const User = require("../api/users");
const Game = require("../api/games");
const Achievement = require("../api/achievements");
const Rewards = require("../api/rewards");
const Counter = require("../api/counters");
const Dialog = require("../api/dialogs");
const Task = require("../api/tasks");
const Increment = require("../api/increments");
const Items = require("../api/items");
const Profile = require("../api/profiles");
const Leaderboard = require("../api/leaderboard");
const Multiplayer = require("../middleware/multiplayer");
const SMS = require("../middleware/sms");
const crate = require("../services/crateio");
const API = require("../middleware/api");
const _ = require("lodash");
const moment = require("moment");
const send = require("@polka/send-type");
const sha = require("../services/sha");

/* Test purposes only */
// router.post('/sms', passport.authenticate('management', { session: false}), (req, res, next) => {
//     log.debug('Received SMS request:', req.body);
//
//     SMS.send(req.body, function (err, ok){
//         res.end('');
//     })
// });

/* Games */
router.post(
  "/games/key",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received api key request:", req.body);

    Game.createApiKey(req, function (err, key) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(key));
    });
  }
);

router.delete(
  "/games/key",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received api key delete request:", req.body);

    Game.deleteApiKey(req, function (err, key) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(key));
    });
  }
);

router.post(
  "/games/create",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received game request:", req.body);

    Game.create(req, function (err, game) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(game));
    });
  }
);

router.post(
  "/games/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received game request:", req.body);

    Game.findwithprivate(req, function (err, game) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(game));
    });
  }
);

/* Achievements */
router.post(
  "/achievements/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received achievements request:", req.body);

    Achievement.findbyprofile(req, function (err, achievement) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(achievement));
    });
  }
);

/* Rewards */
router.post(
  "/rewards/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received rewards request:", req.body);

    Rewards.findbyprofile(req, function (err, reward) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(reward));
    });
  }
);

router.post(
  "/rewards/modify",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received rewards request:", req.body);

    Rewards.modify(req, function (err, reward) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(reward));
    });
  }
);

/* Counters */
router.post(
  "/counters/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received counters request:", req.body);

    Counter.findbyprofile(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.post(
  "/rewards/remove",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received remove reward request:", req.body);

    Rewards.remove(req, function (err, results) {
      res.end(JSON.stringify(results));
    });
  }
);

router.post(
  "/counters/create",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received counters request:", req.body);

    Counter.create(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.put(
  "/counters/modify",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received counters request:", req.body);

    Counter.modify(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.put(
  "/balance",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received counters request:", req.body);

    Counter.modify(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.delete(
  "/counters/remove",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received counters request:", req.body);

    Counter.remove(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

/* Dialogs */
router.post(
  "/dialogs/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received dialogs request:", req.body);

    Dialog.findbyprofile(req, function (err, dialog) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(dialog));
    });
  }
);

router.post(
  "/dialogs/create",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received dialogs request:", req.body);

    Dialog.create(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.put(
  "/dialogs/modify",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received dialogs request:", req.body);

    Dialog.modify(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.delete(
  "/dialogs/remove",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received dialogs request:", req.body);

    Dialog.remove(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

/* Tasks */
router.post(
  "/tasks/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received tasks request:", req.body);

    Task.findbyprofile(req, function (err, task) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(task));
    });
  }
);

router.post(
  "/tasks/create",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received tasks request:", req.body);

    Task.create(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.put(
  "/tasks/modify",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received tasks request:", req.body);

    Task.modify(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.delete(
  "/tasks/remove",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received tasks request:", req.body);

    Task.remove(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

/* Increments */
router.post(
  "/increments/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received increments request:", req.body);

    Increment.findbyprofile(req, function (err, increment) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(increment));
    });
  }
);

/* Items */
router.post(
  "/items/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received items request:", req.body);

    Items.findbyprofile(req, function (err, item) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(item));
    });
  }
);

router.post(
  "/items/createmultiple",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received items request:", req.body);

    Items.createmultiple(req, function (err, item) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(item));
    });
  }
);

/* Profiles */
router.post(
  "/profiles/find",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received profiles request:", req.body);

    Profile.findbyuser(req, function (err, profile) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(profile));
    });
  }
);

router.put(
  "/profiles/modify",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received profiles request:", req.body);

    Profile.modify(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.post(
  "/profiles/ban",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received profiles block request:", req.body);

    Profile.ban(req.body.profile_id, function (err) {
      if (err) return res.end("Failed");

      Game.findwithprivate(req, function (err, game) {
        if (err) return res.end("Failed to get game");

        req.body.game = game;

        Multiplayer.banProfileOnNodes(req, res, function () {
          log.debug(
            "Ban process on nodes completed:",
            req.body.profile_id,
            req.body.game.private.multiplayer.nodes
          );
        });
      });

      res.end("Banned");
    });
  }
);

router.post(
  "/profiles/unban",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received profiles unban request:", req.body);

    Profile.unban(req.body.profile_id, function (err) {
      if (err) return res.end("Failed");

      Game.findwithprivate(req, function (err, game) {
        if (err) return res.end("Failed to get game");

        req.body.game = game;

        Multiplayer.unbanProfileOnNodes(req, res, function () {
          log.debug(
            "Unban process on nodes completed:",
            req.body.profile_id,
            req.body.game.private.multiplayer.nodes
          );
        });
      });

      res.end("Unbanned");
    });
  }
);

router.post(
  "/profiles/block",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received profiles block request:", req.body);

    Profile.block(req.body.profile_id, function (err) {
      if (err) return res.end("Failed");

      Game.findwithprivate(req, function (err, game) {
        if (err) return res.end("Failed to get game");

        req.body.game = game;

        Multiplayer.banProfileOnNodes(req, res, function () {
          log.debug(
            "Ban process on nodes completed:",
            req.body.profile_id,
            req.body.game.private.multiplayer.nodes
          );
        });
      });

      res.end("Blocked");
    });
  }
);

router.post(
  "/profiles/unblock",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received profiles unblock request:", req.body);

    Profile.unblock(req.body.profile_id, function (err) {
      if (err) return res.end("Failed");

      Game.findwithprivate(req, function (err, game) {
        if (err) return res.end("Failed to get game");

        req.body.game = game;

        Multiplayer.unbanProfileOnNodes(req, res, function () {
          log.debug(
            "Unban process on nodes completed:",
            req.body.profile_id,
            req.body.game.private.multiplayer.nodes
          );
        });
      });

      res.end("Unblocked");
    });
  }
);

/* Delete profile for user */
router.post(
  "/profiles/remove",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received remove profiles request:", req.body);

    Profile.remove(req, function (err, results) {
      res.end(JSON.stringify(results));
    });
  }
);

router.post(
  "/leaderboard/periods",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received leaderboard periods request:", req.body);

    //let lastdate = '2022-07-26';
    //let lastmonth = '2022-07';
    //let lastweek = '07-29';
    Leaderboard.getUnmaskedbyDate(
      { body: { game_id: req.body.game_id, name: "points" } },
      req.body.lastdate,
      req.body.lastmonth,
      req.body.lastweek,
      function (err, leaderboard) {
        res.end(JSON.stringify(leaderboard));
      }
    );
  }
);

router.post(
  "/crate/metka",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received data from crate.io:", req.body);

    crate.getRewardsByMetka(req.body.metka, function (err, user) {
      log.info("Got user from crate.io:", user);
      res.end(JSON.stringify(user));
    });
  }
);

router.post(
  "/crate/ctn",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received data from crate.io:", req.body);

    crate.getRewardsByCTN(req.body.ctn, function (err, user) {
      log.info("Got user from crate.io:", user);
      res.end(JSON.stringify(user));
    });
  }
);

router.post(
  "/crate/segment",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received data from crate.io:", req.body);

    crate.getXMAS2023SegmentByCTN(req.body.metka, function (err, user) {
      log.info("Got segment from crate.io:", user);
      res.end(JSON.stringify(user));
    });
  }
);

router.post(
  "/crate/xmas",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received data from crate.io:", req.body);

    crate.getXMAS2023RewardsByCTN(req.body.metka, function (err, user) {
      log.info("Got segment from crate.io:", user);
      res.end(JSON.stringify(user));
    });
  }
);

router.post(
  "/tabby/rewards/createbyprofile",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received data to create rewards manually:", req.body);

    //Sending session result event to a flow
    Game.findwithprivate(req, function (err, game) {
      if (err) return res.end("Failed to get game");

      req.body.game = game;

      _.forEach(req.body.rewards, function (reward) {
        let toIssue = _.find(req.body.game.rewards, { id: reward });
        toIssue["creation_date"] = moment(new Date()).format("DD/MM/YYYY");
        API.publish(
          req.body.profile_id,
          "tabby-attempt",
          {
            game_id: "tabb_devy",
            profile_id: req.body.profile_id,
            player_id: req.body.player_id,
            reward: toIssue,
          },
          function () {
            log.info("Done with:", reward);
          }
        );
      });

      res.end("Done");
    });
  }
);

router.post(
  "/rewards/reissueCoupons",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    res.end("Updated");
    let players = req.body;

    for (let i in players) {
      let partner = players[i];

      switch (partner.activation_type) {
        case "unique": {
          //Getting coupon
          getCoupon("promocodes-" + partner.reward, function (err, promocode) {
            if (err) {
              //No code in stack or error
              console.log("No coupon");
            } else {
              log.warn(" [*] Updating promocode:", partner, promocode);
              //Sending session result event to a flow
              API.publish(
                partner.profile_id,
                "update-promocode",
                {
                  profile_id: partner.profile_id,
                  reward: partner.reward,
                  promocode: promocode,
                  link: partner.link,
                },
                function () {}
              );
            }
          });
          break;
        }

        case "unique_link": {
          getCoupon("promocodes-" + partner.reward, function (err, promocode) {
            if (err) {
              //No code in stack or error
              log.error("Error while getting coupon:", err);
            } else {
              partner.link = partner.link.replace("{{promocode}}", promocode);
              log.warn(" [*] Updating promocode:", partner);

              //Sending session result event to a flow
              API.publish(
                partner.profile_id,
                "update-promocode",
                {
                  profile_id: partner.profile_id,
                  reward: partner.reward,
                  promocode: partner.promocode,
                  link: partner.link,
                },
                function () {}
              );
            }
          });

          break;
        }

        case "unique_nolink": {
          //Getting coupon
          getCoupon("promocodes-" + partner.reward, function (err, promocode) {
            if (err) {
              //No code in stack or error
              log.error(
                "Error while getting coupon:",
                "promocodes-" + partner.reward,
                err
              );
            } else {
              partner.promocode = promocode;
              log.warn(" [*] Updating promocode:", partner);

              //Sending session result event to a flow
              API.publish(
                partner.profile_id,
                "update-promocode",
                {
                  profile_id: partner.profile_id,
                  reward: partner.reward,
                  promocode: promocode,
                  link: partner.link,
                },
                function () {}
              );
            }
          });

          break;
        }

        default: {
          break;
        }
      }
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
);

router.post(
  "/xmas2023/sms",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    log.debug("Received SMS request:", req.body);

    SMS.sendAllOperators(
      {
        profile_id: req.body.ctn,
        player_id: req.body.ctn,
        game_id: req.body.game_id,
        message: req.body.message,
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
              game_id: req.body.game_id,
              message: req.body.message,
              target: "+" + req.body.ctn,
            },
            function () {}
          );

          log.warn("SMS was sent:", req.body.ctn, req.body.message);
          send(res, 200, {});
        }
      }
    );
  }
);

router.post(
  "/healthcheck",
  passport.authenticate("management", { session: false }),
  (req, res, next) => {
    send(res, 200, {
      status: "ok",
    });
  }
);

module.exports = router;
