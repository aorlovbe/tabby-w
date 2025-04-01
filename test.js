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

processCsvFile(filePath).then((results) => {
  results.forEach((el) => {
    const clientId = Object.keys(el);
    /*
    redis.hset('platfform:tabbyw:' + clientId, JSON.stringify(el.clientId))
    */
  });

  console.log(JSON.stringify(results));
});
