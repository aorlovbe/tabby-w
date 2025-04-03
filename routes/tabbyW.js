const polka = require("polka");
const router = polka();
const passport = require("../middleware/passport-auth");
const log = require("../services/bunyan").log;
const API = require("../middleware/api");
const Counter = require("../api/counters");
const send = require("@polka/send-type");
const redis = require("../services/redis").redisclient_rewarder;
const _ = require("lodash");
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const { getUserInfo } = require("../middleware/acelera-methods");

router.post(
  "/settings",
  passport.authenticate("api", { session: false }),
  API.getGame,
  API.Counters,
  async (req, res, next) => {
    send(res, 200, {
      status: "ok",
      onboarding: true,
      attempts: req.body.counters.attempt,
      prizes: ["r-1", "r-2", "r-3", "r-4", "r-5", "r-6", "r-7", "r-1", "r-1"],
    });
  }
);

router.post(
  "/tasks",
  passport.authenticate("api", { session: false }),
  API.getGame,
  async (req, res, next) => {
    try {
      log.info("get tasks for:", req.body.player_id, req.body.game_id);

      const tasks = await getUserInfo(
        "tasks",
        req.body.player_id,
        req.body.game_id
      );

      send(res, 200, {
        status: "ok",
        tasks,
      });
    } catch (error) {
      log.error("Error with getting tasks", error);
      return send(res, 500, {
        status: "failed",
        tasks: [],
      });
    }
  }
);

router.put(
  "/balance",
  passport.authenticate("api", { session: false }),
  API.Counters,
  async (req, res, next) => {
    log.debug("Received counters request:", req.body);
    if (req.body.counters["attempt"] === undefined) {
      const attempt = await new Promise((resolve, reject) =>
        Counter.create(
          {
            body: {
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              name: "attempt",
              value: 100,
            },
          },
          function (err, attempt) {
            err ? reject(err) : resolve(attempt);
          }
        )
      );
      req.body.counters["attempt"] = attempt["attempt"];
    }

    Counter.modify(req, function (err, counter) {
      if (err) return res.end("Failed");
      res.end(JSON.stringify(counter));
    });
  }
);

router.post(
  "/step",
  passport.authenticate("api", { session: false }),
  API.getGame,
  API.Counters,
  async (req, res, next) => {
    try {
      if (req.body.player_id === undefined) {
        return send(res, 500, {
          status: "failed",
        });
      }
      if (req.body.counters.attempt === undefined) {
        const attempt = await new Promise((resolve, reject) =>
          Counter.create(
            {
              body: {
                game_id: req.body.game.game_id,
                player_id: req.body.player_id,
                name: "attempt",
                value: 100,
              },
            },
            function (err, attempt) {
              err ? reject(err) : resolve(attempt);
            }
          )
        );

        req.body.counters.attempt = attempt["attempt"];
      }
      const attempt = await new Promise((resolve, reject) =>
        Counter.modify(
          {
            body: {
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              name: "attempt",
              value: -1,
            },
          },
          function (err, attempt) {
            err ? reject(err) : resolve(attempt);
          }
        )
      );

      req.body.counters.attempt = attempt["attempt"];

      const roll = Math.floor(Math.random() * 100) + 1;
      let reward;

      // if (roll <= 2) {
      //   reward = ["r-6", "r-7"][Math.floor(Math.random() * 2)];
      // } else {
      //   reward = ["r-1", "r-2", "r-3", "r-4", "r-5"][
      //     Math.floor(Math.random() * 5)
      //   ];
      // }

      send(res, 200, {
        status: "ok",
        attempts: req.body.counters.attempt,
        // prize: _.cloneDeep(reward),
        prize: "r-1",
      });
    } catch (error) {
      send(res, 500, {
        status: "failed",
      });
    }
  }
);

router.post("/healthcheck", (req, res, next) => {
  log.warn(
    "Полет нормальный в",
    moment(momentTimezone.tz("Europe/Moscow")._d).format("YYYY-MM-DD HH:mm:ss")
  );
  send(res, 200, {
    status: "ok",
  });
});

module.exports = router;
