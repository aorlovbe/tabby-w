const redis = require("../services/redis").redisclient;
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
var glob = require("glob");
// const { log } = require("../services/bunyan");
const BATCH_SIZE = 50;
// let schedule = "* * * * * *";
let schedule = "55 59 23 * * 0";
const CronJob = require("cron").CronJob;
const { promisify } = require("util");
const rename = promisify(fs.rename);

async function renameFile(filePath) {
  const splitPath = filePath.split("/");
  const newFileName = "parsed_" + splitPath[2];
  const newPath = splitPath[0] + "/" + splitPath[1] + "/" + newFileName;
  try {
    await rename(filePath, newPath);
    console.log("Файл успешно переименован", newFileName);
  } catch (error) {
    console.error("Ошибка при переименовании файла:", error);
  }
}

async function processCsvFile(filePath, batch = 0) {
  try {
    const result = [];
    fs.createReadStream(filePath, { encoding: "utf-8" })
      .pipe(csv())
      .on("data", (record) => {
        console.log(record);
        const clientId = record.client_id;
        console.log(clientId);
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
        await renameFile(filePath);
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

let job = new CronJob(schedule, function () {
  processCsvFile("ftp/download_from_tabby/customer_task_eligibility.csv", 0);
});

// processCsvFile("ftp/download_from_tabby/customer_task_eligibility.csv", 0);

job.start();
