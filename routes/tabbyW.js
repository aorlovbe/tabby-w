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
// const usersTasks = require("../customer_task_eligibility.csv");
const rateLimit = require("express-rate-limit");
const accelera = require("../services/producer");
const Achievements = require("../api/achievements");
const timeZone = require("moment-timezone");
const nanoid = require("../services/nanoid");
const {
  // createDefaultUserCounters,
  addAttemptsToDonor,
} = require("../middleware/gameconfig");
const fs = require("fs");
const csv = require("csv-parser");

//Services activation from games (with token)
router.post(
  "/settings",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  // API.Counters,
  // API.isBlockedClient,
  // API.isBlocked,
  // API.isBlockedIP,
  async (req, res, next) => {
    // await createDefaultUserCounters(req);

    const availablePrizes =
      "[]Взять последний список, найти пользователя, если тру, добавить приз в массив";
    const availableTasks =
      "[]Взять последний список, найти пользователя, если тру, добавить задание в массив";

    // send(res, 200, {
    //   status: "ok",
    //   onboarding: req.body.counters.onboarding === "true",
    //   attempts: req.body.counters.attempt,
    //   referalCode: req.body.token.profile_id,
    //   prizes: [availablePrizes],
    //   tasks: [availableTasks],
    // });
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
  "/onboarding-completed",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  API.isBlockedClient,
  API.isBlocked,
  API.isBlockedIP,
  async (req, res, next) => {
    // if (
    //   req.body.counters.onboarding === undefined ||
    //   req.body.counters.onboarding === "true"
    // ) {
    //   const onboarding = await new Promise((resolve, reject) =>
    //     Counter.create(
    //       {
    //         body: {
    //           game_id: req.body.game.game_id,
    //           profile_id: req.body.profile_id,
    //           name: "onboarding",
    //           value: "false",
    //         },
    //       },
    //       function (err, onboarding) {
    //         err ? reject(err) : resolve(onboarding);
    //       }
    //     )
    //   );

    //   req.body.counters.onboarding = onboarding["onboarding"];
    // }
    // let event = {
    //   event: "game",
    //   page: "onboarding",
    //   status: "finish",
    //   game_id: req.body.game.game_id,
    //   player_id:
    //     req.body.player_id === undefined ? "" : req.body.player_id.toString(),
    //   timestamp: Math.floor(new Date()),
    //   date: moment(new Date()).format("YYYY-MM-DD"),
    //   time: moment(new Date()).format("HH:mm"),
    //   datetime: moment(momentTimezone.tz("Europe/Moscow")).format(
    //     "YYYY-MM-DD HH:mm:ss"
    //   ),
    // };

    // bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
    //   if (err) {
    //     log.error(
    //       "Error while storing webhooks messages for Clickhouse bulk:",
    //       err
    //     );
    //   }
    // });
    send(res, 200, {
      status: "ok",
    });
  }
);

router.post(
  "/step",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  // API.Counters,
  // API.isBlockedClient,
  // API.isBlocked,
  // API.isBlockedIP,
  async (req, res, next) => {
    // const attempt = await new Promise((resolve, reject) =>
    //   Counter.create(
    //     {
    //       body: {
    //         game_id: req.body.game.game_id,
    //         profile_id: req.body.profile_id,
    //         name: "attempt",
    //         value: -1,
    //       },
    //     },
    //     function (err, attempt) {
    //       err ? reject(err) : resolve(attempt);
    //     }
    //   )
    // );

    // req.body.counters.attempt = attempt["attempt"];
    // let event = {
    //   event: "game",
    //   page: "onboarding",
    //   status: "finish",
    //   game_id: req.body.game.game_id,
    //   player_id:
    //     req.body.player_id === undefined ? "" : req.body.player_id.toString(),
    //   timestamp: Math.floor(new Date()),
    //   date: moment(new Date()).format("YYYY-MM-DD"),
    //   time: moment(new Date()).format("HH:mm"),
    //   datetime: moment(momentTimezone.tz("Europe/Moscow")).format(
    //     "YYYY-MM-DD HH:mm:ss"
    //   ),
    // };

    // bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
    //   if (err) {
    //     log.error(
    //       "Error while storing webhooks messages for Clickhouse bulk:",
    //       err
    //     );
    //   }
    // });
    send(res, 200, {
      status: "ok",
      // attempts: req.body.counters.attempt,
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

// router.post(
//   "/tasks",
//   passport.authenticate("api", { session: false }),
//   API.getGame,
//   Token.Decrypt,
//   (req, res, next) => {
//     // Task.findbyprofile(req, function (err, task) {
//     //   if (err)
//     //     return send(res, 500, { status: "failed", active: [], completed: [] });

//     //   let active = task.active === undefined ? [] : task.active;
//     //   let completed = task.completed === undefined ? [] : task.completed;

//     //   let active_replaced = [];
//     //   let completed_replaced = [];

//     //   let i = 0;
//     //   _.forEach(active, function (value) {
//     //     let global_details = _.find(req.body.game.tasks, { name: value.name });

//     //     if (global_details !== undefined) {
//     //       value.full_description =
//     //         global_details.full_description !== undefined
//     //           ? global_details.full_description
//     //           : value.full_description;
//     //       value.short_description =
//     //         global_details.short_description !== undefined
//     //           ? global_details.short_description
//     //           : value.short_description;
//     //       value.link_1_link =
//     //         global_details.link_1_link !== undefined
//     //           ? global_details.link_1_link
//     //           : value.link_1_link;
//     //       value.link_1_desc =
//     //         global_details.link_1_desc !== undefined
//     //           ? global_details.link_1_desc
//     //           : value.link_1_desc;
//     //       value.link_2_link =
//     //         global_details.link_2_link !== undefined
//     //           ? global_details.link_2_link
//     //           : value.link_2_link;
//     //       value.link_2_desc =
//     //         global_details.link_2_desc !== undefined
//     //           ? global_details.link_2_desc
//     //           : value.link_2_desc;
//     //       value.link_3_link =
//     //         global_details.link_3_link !== undefined
//     //           ? global_details.link_3_link
//     //           : value.link_3_link;
//     //       value.link_3_desc =
//     //         global_details.link_3_desc !== undefined
//     //           ? global_details.link_3_desc
//     //           : value.link_3_desc;
//     //       value.link_4_link =
//     //         global_details.link_4_link !== undefined
//     //           ? global_details.link_4_link
//     //           : value.link_4_link;
//     //       value.link_4_desc =
//     //         global_details.link_4_desc !== undefined
//     //           ? global_details.link_4_desc
//     //           : value.link_4_desc;
//     //       value.achievement = global_details.achievement;
//     //       active_replaced.push(value);
//     //     }
//     //     i++;
//     //   });

//     //   if (i === active.length) {
//     //     let j = 0;
//     //     _.forEach(completed, function (value) {
//     //       let global_details = _.find(req.body.game.tasks, {
//     //         name: value.name,
//     //       });

//     //       value.full_description = global_details.full_description;
//     //       value.short_description = global_details.short_description;
//     //       value.achievement = global_details.achievement;
//     //       completed_replaced.push(value);

//     //       j++;
//     //     });

//     //     if (j === completed.length) {
//     //       send(res, 200, {
//     //         status: "ok",
//     //         active: _.cloneDeep(active_replaced),
//     //         completed: _.cloneDeep(completed_replaced),
//     //       });
//     //     }
//     //   }
//     // });
//   }
// );

router.post(
  "/referal",
  passport.authenticate("api", { session: false }),
  API.getGame,
  Token.Decrypt,
  API.Counters,
  API.isBlockedClient,
  API.isBlocked,
  API.isBlockedIP,
  async (req, res, next) => {
    const status = await addAttemptsToDonor(req);

    //TODO create publishTrigger

    if (!status) {
      send(res, 500, {
        status: "failed",
      });
    }

    send(res, 200, {
      status: "ok",
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
