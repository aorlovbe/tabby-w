const csv = require("../services/csv-basic");
const _ = require("lodash");
const glob = require("glob");
const path = require("path");
const fs = require("fs");
const Promise = require("bluebird");
const log = require("../services/bunyan").log;
const bulk = require("../services/bulk");
const moment = require("moment");
const timeZone = require("moment-timezone");
const producer = require("../services/producer");
const settings = require("../settings");
const API = require("../middleware/api");
const Bottleneck = require("bottleneck");

// Конфигурация ограничения скорости
const RATE_LIMIT = settings.publishRateLimit || 20; // сообщений в секунду
const RATE_LIMIT_INTERVAL = 1000 / RATE_LIMIT;

const limiter = new Bottleneck({
  minTime: RATE_LIMIT_INTERVAL,
  maxConcurrent: 1,
});

/* ------------------------------------------------------------- */
// Запуск продюсера и планировщика
producer.createProducer(settings.instance).then(function () {
  log.info("Worker started:", settings.instance);
  start();
  setInterval(start, 60000); //
});

// Отправка события через API с лимитом
function enqueueEvent(item) {
  limiter.schedule(() => {
    return new Promise((resolve) => {
      API.publish(item.client_id, item.event_name, item.data, (err) => {
        if (err) {
          log.error("Publish error:", err);
        }
        resolve();
      });
    });
  });
}

// Обработка одного файла CSV
function events(target) {
  csv.parse(
    target,
    ",",
    (err, rows, result) => {
      if (err) {
        log.error("Error parsing row:", err.message);
        return;
      }

      try {
        if (
          !result ||
          !result.client_id ||
          !result.task_id ||
          !result.completion_timestamp
        ) {
          log.warn("Skipping row with missing required fields:", result);
          return;
        }

        if (
          _.isEmpty(result.client_id) ||
          _.isEmpty(result.task_id) ||
          _.isEmpty(result.completion_timestamp)
        ) {
          log.warn("Skipping row with empty required fields:", result);
          return;
        }

        const taskNumberMatch = result.task_id.match(/task_(\d+)/);
        if (!taskNumberMatch || !taskNumberMatch[1]) return;

        const taskNumber = taskNumberMatch[1];
        const eventName = `task-${taskNumber}-completed`;

        const out = {
          requestID: result.client_id,
          name: eventName,
          load_dttm: result.completion_timestamp,
        };

        // Отправка события через ограниченную очередь
        enqueueEvent({
          client_id: result.client_id,
          event_name: eventName,
          data: out,
        });

        // Логируем в bulk
        bulk.store(
          "tabby",
          JSON.stringify({
            timestamp: Math.floor(Date.now()),
            profile_id: result.player_id || "unknown",
            game_id: "tabby",
            event: "accelera-api",
            page: "tasks-worker",
            status: "processed",
            additional: JSON.stringify(out),
            datetime: moment(timeZone.tz("Europe/Moscow")).format(
              "YYYY-MM-DD HH:mm:ss"
            ),
          }),
          (err) => {
            if (err) log.error("Error storing bulk data:", err);
          }
        );
      } catch (error) {
        log.error("Error processing row:", error, "Row data:", result);
      }
    },
    (done) => {
      log.info("Done with file:", target);
    }
  );
}

// Поиск и запуск обработки файлов
function start() {
  glob(
    path.join(
      __dirname,
      "../ftp/download_from_tabby",
      "@(ready_customer_task_completion_*)"
    ),
    (er, files) => {
      if (er) {
        log.error("Error finding files:", er);
        return;
      }

      if (files.length !== 0) {
        log.info(`Found ${files.length} files to process`);

        Promise.each(files, (file) => {
          return new Promise((resolve) => {
            try {
              events(file);
              resolve();
            } catch (err) {
              log.error("Error processing file:", file, err);
              resolve();
            }
          });
        })
          .then(() => log.info("Finished processing all files"))
          .catch((err) => log.error("Processing error:", err));
      } else {
        log.warn("No files found to parse");
      }
    }
  );
}
