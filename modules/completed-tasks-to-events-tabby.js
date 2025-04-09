const csv = require("../services/csv-basic");
const _ = require("lodash");
var glob = require("glob");
const path = require("path");
const fs = require("fs");
const Promise = require("bluebird");
const log = require("../services/bunyan").log;
let bulk = require("../services/bulk");
const moment = require("moment");
const timeZone = require("moment-timezone");
const producer = require("../services/producer");
const settings = require("../settings");
const API = require("../middleware/api");

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer and Game API events consumer */
producer.createProducer(settings.instance).then(function () {
  log.info(
    "Accelera Game API tabby tasks to event worker is created:",
    settings.instance
  );
  //Starting schedule
  start();

  setInterval(function () {
    start();
  }, 1000 * 60);
});

function events(target) {
  csv.parse(
    target,
    ";",
    (err, rows, result) => {
      //Transformation function
      if (err) return log.error(err.message);

      if (result.client_id !== "") {
        console.log(result);
        let out = {
          requestID: result.profile_id,
          name: "task-" + result.task_id.split("_")[1],
          load_dttm: result.completion_timestamp,
        };

        log.warn("Processed task:", out);

        // console.log("task-" + result.task_id.split("_")[1] + "-completed");
        //Publish trigger
        API.publish(
          result.client_id,
          "task-" + result.task_id.split("_")[1] + "-completed",
          out,
          function () {}
        );

        bulk.store(
          "tabby_dev",
          JSON.stringify({
            timestamp: Math.floor(new Date()),
            profile_id: result.player_id,
            game_id: "tabby",
            event: "accelera-api",
            page: "tasks-worker",
            status: "processed",
            additional: JSON.stringify(out),
            date: moment(timeZone.tz("Europe/Moscow")).format("YYYY-MM-DD"),
            time: moment(timeZone.tz("Europe/Moscow")).format("HH:mm"),
            datetime: moment(timeZone.tz("Europe/Moscow")._d).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          }),
          function () {}
        );
      }
    },
    (done) => {
      log.info("Done with a file:", target);
    }
  );
}

function start() {
  glob(
    path.join(
      __dirname,
      "../ftp/download_from_tabby",
      "@(customer_task_completion_*)"
    ),
    function (er, files) {
      if (files.length !== 0) {
        log.warn("Found tasks files:", files.length);

        Promise.each(files, function (file) {
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
