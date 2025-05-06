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
    let country = req.body.country;
    let now = moment(new Date()).format("DD-MM-YYYY");
    if (req.body.counters.last_signin === undefined) {
      const last_signin = await new Promise((resolve, reject) =>
        Counter.create(
          {
            body: {
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              name: "last_signin",
              value: now,
            },
          },
          function (err, last_signin) {
            err ? reject(err) : resolve(last_signin);
          }
        )
      );
      req.body.counters.last_signin = last_signin["last_signin"];
    }

    let rewardsPool = [
      "r-1",
      "r-2",
      "r-3",
      "r-4",
      "r-5",
      country === "ARE" ? "r-6" : "r-7",
      "r-8",
      "r-9",
      "r-10",
      country === "ARE" ? "r-11" : "r-12",
    ];

    if (
      req.body.counters.attempt === undefined ||
      req.body.counters.attempt < 0
    ) {
      const attempt = await new Promise((resolve, reject) =>
        Counter.create(
          {
            body: {
              game_id: req.body.game.game_id,
              player_id: req.body.player_id,
              name: "attempt",
              value: 1,
            },
          },
          function (err, attempt) {
            err ? reject(err) : resolve(attempt);
          }
        )
      );

      req.body.counters.attempt = attempt["attempt"];
    }

    send(res, 200, {
      status: "ok",
      attempts: req.body.counters.attempt,
      prizes: _.cloneDeep(rewardsPool),
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

      const tasks = await getUserInfo("tasks", req.body.player_id, "tabby");

      if (tasks === undefined) {
        return send(res, 500, {
          status: "failed",
          tasks: [],
        });
      }

      redis.hget(
        "platform:profile:tasks",
        req.body.player_id,
        (err, result) => {
          if (result !== null) {
            let usersTasks = result.split("_").join("-");

            const usersAvailableTasks = tasks.filter((el) =>
              usersTasks.includes(el.id)
            );

            send(res, 200, {
              status: "ok",
              tasks: _.cloneDeep(usersAvailableTasks),
            });
          } else {
            const commonUsersTask = ["task-5", "task-6", "task-7", "task-8"];

            const usersCommonFilteredTasks = tasks.filter((el) =>
              commonUsersTask.includes(el.id)
            );
            return send(res, 200, {
              status: "ok",
              tasks: _.cloneDeep(usersCommonFilteredTasks),
            });
          }
        }
      );
    } catch (err) {
      const errorMessage = {
        status: err.response ? err.response.status : "N/A",
        message: err.message,
        method: err.config.method,
        url: err.config.url,
        data: err.response ? err.response.data : "Нет данных",
        errorType: err.name,
      };

      log.error("Error with getting tasks", errorMessage);
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

    if (
      req.body.counters["attempt"] === undefined ||
      req.body.counters.attempt < 0
    ) {
      const attempt = await new Promise((resolve, reject) =>
        Counter.create(
          {
            body: {
              game_id: "tabby",
              player_id: req.body.player_id,
              name: "attempt",
              value: 1,
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
      if (err) return res.end("failed");
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
      let now = moment(new Date()).format("DD-MM-YYYY");
      let country = req.body.country;
      let reward;

      if (req.body.counters.last_signin !== now) {
        return send(res, 500, {
          status: "failed",
        });
      }

      if (req.body.player_id === undefined) {
        return send(res, 500, {
          status: "failed",
        });
      }

      if (
        req.body.counters.attempt === undefined ||
        req.body.counters.attempt < 0
      ) {
        const attempt = await new Promise((resolve, reject) =>
          Counter.create(
            {
              body: {
                game_id: req.body.game.game_id,
                player_id: req.body.player_id,
                name: "attempt",
                value: 1,
              },
            },
            function (err, attempt) {
              err ? reject(err) : resolve(attempt);
            }
          )
        );

        req.body.counters.attempt = attempt["attempt"];
      }
      if (Number(req.body.counters.attempt) > 0) {
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

        let rewardsPool = [
          "r-1",
          "r-2",
          "r-3",
          "r-4",
          "r-5",
          country === "ARE" ? "r-6" : "r-7",
          "r-8",
          "r-9",
          "r-10",
          country === "ARE" ? "r-11" : "r-12",
        ];

        if (Number(req.body.counters.attempt) >= 0) {
          const roll = Math.floor(Math.random() * 100);
          if (roll >= 98) {
            reward = rewardsPool[rewardsPool.length - 1];
          } else {
            const partSize = 97 / (rewardsPool.length - 1);
            const index = Math.min(
              Math.floor(roll / partSize),
              rewardsPool.length - 2
            );

            reward = rewardsPool[index];
          }
        }

        send(res, 200, {
          status: "ok",
          attempts: req.body.counters.attempt,
          prize: _.cloneDeep(reward),
        });
      } else {
        return send(res, 500, {
          status: "failed",
        });
      }
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
