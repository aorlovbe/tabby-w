const { promisify } = require("util");
const redis = require("../services/redis").redisclient;
const pipeline = promisify(redis.pipeline).bind(redis);
const infoRedis = promisify(redis.info).bind(redis);

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

async function batchInsertIntoRedis(
  data,
  batchSize = 1000,
  monitoringInterval = 1000
) {
  const pipelineBatch = pipeline();
  let count = 0;
  let monitoringTimer;

  const startMonitoring = async () => {
    monitoringTimer = setInterval(async () => {
      const redisInfo = await infoRedis();
      const memoryUsage = redisInfo["used_memory_human"];
      const connectedClients = redisInfo["connected_clients"];
      const instantOps = redisInfo["instant_metrics"];
      const keyspace = redisInfo["keyspace"];

      console.log(`Мониторинг Redis:
                    Память: ${memoryUsage}
                    Подключенные клиенты: ${connectedClients}
                    Операции в секунду: ${instantOps}
                    Количество ключей: ${keyspace}`);
    }, monitoringInterval);
  };

  const stopMonitoring = () => {
    clearInterval(monitoringTimer);
  };

  startMonitoring();

  for (const el of data) {
    const clientId = Object.keys(el)[0];
    const tasks = JSON.stringify(el[clientId]);

    redis.hset(`platfform:profile:${clientId}:tasks`, clientId, tasks);

    count++;
    if (count % batchSize === 0) {
      await pipelineBatch.exec();
      pipelineBatch.reset();
      await new Promise((resolve) => setTimeout(resolve, 100));
    }
  }

  // Обработка оставшихся записей
  if (count % batchSize !== 0) {
    await pipelineBatch.exec();
  }

  stopMonitoring();
}

(async () => {
  const results = await processCsvFile(
    "../ftp/upload_from_tabby/customer_task_eligibility.csv"
  );
  await batchInsertIntoRedis(results);
  console.log(JSON.stringify(results));
})();

/*
три типа файлов. 
customer_task_eligibility - большой - 1р в день
customer_reward_eligibility - большой - 1р в день
customer_task_completion - постоянные файлы с выполнением
*/
