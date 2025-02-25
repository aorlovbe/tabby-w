const csv = require("../services/csv");
let redis = require("../services/redis").redisclient;
const _ = require("lodash");
var glob = require("glob");
const path = require("path");
const fs = require("fs");
const Promise = require("bluebird");
const log = require("../services/bunyan").log;
let bulk = require("../services/bulk");
const moment = require("moment");
const timeZone = require("moment-timezone");

function events(target) {
  csv.parse(
    target,
    ";",
    (err, rows, result) => {
      //Transformation function
      //console.log(result);
      let REWARDID = path.basename(target).split("_")[1];

      if (Object.values(result)[0] !== "") {
        redis.lpush(
          "platform:coupons:promocodes-" + REWARDID,
          Object.values(result)[0],
          function (err, ok) {}
        );
      } else {
        log.warn("Found empty code", REWARDID);
      }
    },
    (done) => {
      log.warn("Done with a file:", target);
    }
  );
}

function start() {
  glob(
    path.join(__dirname, "../promocodes", "@(orange*)"),
    function (er, files) {
      if (files.length !== 0) {
        log.warn("Found coupons files:", files.length);

        let sample = files.splice(0, 5);
        Promise.each(sample, function (file) {
          return events(file);
        })
          .then(function (result) {})
          .catch(function (err) {
            log.error("Got error while processing mission files:", err);
          });
      } else {
        log.warn("Nothing to parse");
      }
    }
  );
}

start();
