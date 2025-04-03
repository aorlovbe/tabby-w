const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const filePath = path.join(__dirname, "./customer.csv");

async function processCsvFile(filePath) {
  try {
    const results = [];

    const records = await new Promise((resolve, reject) => {
      const results = [];

      fs.createReadStream(filePath)
        .pipe(csv())
        .on("data", (data) => results.push(data))
        .on("error", reject)
        .on("end", () => resolve(results));
    });

    records.forEach((record) => {
      const tasks = Object.entries(record)
        .slice(1)
        .filter(([, value]) => value === "True")
        .map(([key]) => key);

      results.push({
        [record.client_id]: tasks,
      });
    });

    return results;
  } catch (error) {
    console.error("Ошибка при обработке файла:", error);
    return [];
  }
}

processCsvFile(filePath).then(("ftp/upload_from_tabby/customer_task_eligibility.csv") => {
  results.forEach((el) => {
    const clientId = Object.keys(el);

    redis.hset(
      "platfform:profile:" + clientId,
      JSON.stringify(el.clientId) + "tasks"
    );
  });

  console.log(JSON.stringify(results));
});
/*
 */
