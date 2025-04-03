// const { promisify } = require("util");
const redis = require("../services/redis").redisclient;
// const pipeline = promisify(redis.pipeline).bind(redis);
// const infoRedis = promisify(redis.info).bind(redis);
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
// const { log } = require("../services/bunyan");
const BATCH_SIZE = 500;

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

processCsvFile("ftp/upload_from_tabby/customer_task_eligibility.csv", 0);
