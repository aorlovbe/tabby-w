const polka = require("polka");
const router = polka();
const passport = require("../middleware/passport-auth");
const log = require("../services/bunyan").log;
const API = require("../middleware/api");
const Counter = require("../api/counters");
const send = require("@polka/send-type");
const redis = require("../services/redis").redisclient_rewarder;
const moment = require("moment");
const momentTimezone = require("moment-timezone");

router.post(
  "/settings",
  passport.authenticate("api", { session: false }),
  API.getGame,
  (req, res, next) => {
    send(res, 200, {
      status: "ok",
      onboarding: false,
      attempts: 1,
      prizes: [
        "pk-1",
        "pk-2",
        "pk-3",
        "pk-4",
        "pk-5",
        "pk-6",
        "pk-7",
        "pk-1",
        "pk-1",
      ],
    });
  }
);

router.post(
  "/tasks",
  passport.authenticate("api", { session: false }),
  API.getGame,
  (req, res, next) => {
    send(res, 200, {
      status: "ok",
      history: [
        {
          id: "svyznoy",
          status: "completed",
          task_type: "link",
          btn_name: "Complete",
          title: "7 дней Литрес за 0 ₽",
          short_description: "45 дней подписки для всей семьи дней подписки",
          full_description:
            "чтобы ваши будни были ярче, а настроение прекрасней - дарим серебряную подвеску от SOKOLOV. Порадуйте себя или близких",
        },
      ],
      active: [
        {
          id: "pk-17",
          task_type: "attempt",
          title: "7 дней Литрес за 0 ₽",
          short_description: "45 дней подписки для всей семьи дней подписки",
          full_description: "",
          link: "https://cloudbeeline.ru/offers/642fef4be398adf91f0e20b8",
          btn_name: "Take it!",
        },
        {
          id: "treasure_partner",
          short_description: "Фильм о новогоднем волшебстве в подарок",
          full_description:
            "чтобы ваши будни были ярче, а настроение прекрасней - дарим серебряную подвеску от SOKOLOV. Порадуйте себя или близких",

          btn_name: "Complete",
          link: "https://beeline.tv/settings/coupons/",
          title: "Рождественская история",
          task_type: "link",
        },
      ],
    });
  }
);

router.put(
  "/balance",
  passport.authenticate("api", { session: false }),
  API.Counters,
  async (req, res, next) => {
    log.debug("Received counters request:", req.body);
    // const counterName = req.body.name;
    if (req.body.counters["attempt"] === undefined) {
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
    if (req.body.counters.attempt) {
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

    send(res, 200, {
      status: "ok",
      attempts: req.body.counters.attempt,
      prize: {
        id: "pk-4",
        promocode: "56yndn_jj6nh_4h",
        btn_name: "Take it!",
        title: "Headline: prize name",
        short_description: "45 дней подписки для всей семьи дней подписки",
        full_description:
          "чтобы ваши будни были ярче, а настроение прекрасней - дарим серебряную подвеску от SOKOLOV. Порадуйте себя или близких",
        link: "https://cloudbeeline.ru/offers/642fef4be398adf91f0e20b8",
      },
    });
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
