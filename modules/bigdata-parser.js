const redis = require("../services/redis").redisclient;
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
var glob = require("glob");
// const { log } = require("../services/bunyan");
const BATCH_SIZE = 50;

// setInterval(function () {
//   log.warn("Starting sftp2 session for Tabby / download");
//   start();
// }, 1000 * 60 * 60 * 24);

// start();

async function processCsvFile(filePath, batch = 0) {
  try {
    const result = [];
    fs.createReadStream(filePath, { encoding: "utf-8" })
      .pipe(csv())
      .on("data", (record) => {
        const clientId = record.client_id;
        const tasks = Object.entries(record)
          .slice(1)
          .filter(([, value]) => value === "True")
          .map(([key]) => key);

        result.push({ clientId, tasks });
      })
      .on("error", (error) => console.error(error))
      .on("end", async () => {
        for (let i = batch; i < result.length; i += BATCH_SIZE) {
          try {
            await Promise.all(
              result
                .slice(i, i + BATCH_SIZE)
                .map((record) =>
                  hSet(
                    `platform:profile:tasks`,
                    record.clientId,
                    JSON.stringify(record.tasks)
                  )
                )
            );

            console.log("Saved lines:", i, i + BATCH_SIZE);
          } catch (error) {
            console.error("Error with batch:", i);
            console.error("Error:", error);
            process.exit(1);
          }
        }
        console.log("DONE");
        process.exit(0);
      });
  } catch (error) {
    console.error("Ошибка при обработке файла:", error);
    return [];
  }
}

const hSet = (key, client_id, tasks) =>
  new Promise((resolve, reject) =>
    redis.hset(key, client_id, tasks, (err, num) => {
      if (err) {
        return reject(err);
      }
      return resolve(num);
    })
  );

await processCsvFile("ftp/upload_from_tabby/customer_task_eligibility.csv", 0);

// function start() {
//   glob(
//     path.join(
//       __dirname,
//       "../ftp/download_from_tabby",
//       "@(customer_task_eligibility*)"
//     ),
//     async function (er, files) {
//       if (files.length !== 0) {
//         log.warn("Found tasks files:", files.length);

//         await processCsvFile(
//           "ftp/upload_from_tabby/customer_task_eligibility.csv",
//           0
//         );
//       } else {
//         log.warn("Nothing to parse");
//       }
//     }
//   );
// }
