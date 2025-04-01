const commonConfig = require("../gaming-files/cbmap.json");
let _ = require("lodash");
const moment = require("moment");
const timeZone = require("moment-timezone");
const momentTimezone = require("moment-timezone");
const Counter = require("../api/counters");
const Profiles = require("../api/profiles");

const createDefaultUserCounters = async (req) => {
  if (req.body.counters.attempts === undefined) {
    const attempts = await new Promise((resolve, reject) =>
      Counter.create(
        {
          body: {
            game_id: req.body.game.game_id,
            profile_id: req.body.profile_id,
            name: "attempts",
            value: 1,
          },
        },
        function (err, attempts) {
          err ? reject(err) : resolve(attempts);
        }
      )
    );
    req.body.counters.attempts = attempts["attempts"];
  }

  if (req.body.counters.onboarding === undefined) {
    const onboarding = await new Promise((resolve, reject) =>
      Counter.create(
        {
          body: {
            game_id: req.body.game.game_id,
            profile_id: req.body.profile_id,
            name: "onboarding",
            value: "true",
          },
        },
        function (err, onboarding) {
          err ? reject(err) : resolve(onboarding);
        }
      )
    );
    req.body.counters.onboarding = onboarding["onboarding"];
  }
};

const donorCounters = syn;

const addAttemptsToDonor = async (req) => {
  const donorProfileId = req.body.context.referalCode;

  const profile_id = req.body.body.profile_id;
  if (donorProfileId === profile_id) {
    return false;
  }

  if (req.body.counters.isUseReferal === "true") {
    return false;
  }

  const attempts = await new Promise((resolve, reject) =>
    Counter.modify(
      {
        body: {
          game_id: req.body.game.game_id,
          profile_id: donorProfileId,
          name: "attempts",
          value: 1,
        },
      },
      function (err, attempts) {
        err ? reject(err) : resolve(attempts);
      }
    )
  );
  req.body.counters.attempts = attempts["attempts"];

  const isUseReferal = await new Promise((resolve, reject) =>
    Counter.create(
      {
        body: {
          game_id: req.body.game.game_id,
          profile_id: profile_id,
          name: "isUseReferal",
          value: "true",
        },
      },
      function (err, isUseReferal) {
        err ? reject(err) : resolve(isUseReferal);
      }
    )
  );
  req.body.counters.isUseReferal = isUseReferal["isUseReferal"];
};

module.exports = {
  createDefaultUserCounters,
  addAttemptsToDonor,
};
