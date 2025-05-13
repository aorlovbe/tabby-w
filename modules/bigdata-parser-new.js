const redis = require("../services/redis").redisclient;
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const { promisify } = require("util");
const pipeline = promisify(redis.pipeline).bind(redis);
const unlink = promisify(fs.unlink);
const rename = promisify(fs.rename);
const CronJob = require("cron").CronJob;

// Конфигурация
const config = {
  inputFile: "ftp/download_from_tabby/customer_task_eligibility.csv",
  batchSize: 100, // Увеличено для оптимизации Redis
  maxParallelBatches: 5, // Ограничение параллельных запросов к Redis
};

async function safeDeleteFile(filePath) {
  try {
    await unlink(filePath);
    console.log(`Файл ${filePath} успешно удалён`);
  } catch (error) {
    console.error(`Ошибка при удалении файла ${filePath}:`, error);
  }
}

async function renameProcessedFile(filePath) {
  const dir = path.dirname(filePath);
  const filename = path.basename(filePath);
  const newPath = path.join(dir, `parsed_${filename}`);

  try {
    await rename(filePath, newPath);
    console.log(`Файл переименован в ${newPath}`);
  } catch (error) {
    console.error("Ошибка при переименовании файла:", error);
    throw error; // Прерываем выполнение при ошибке
  }
}

async function processBatch(batch) {
  try {
    const pipe = pipeline();
    batch.forEach(({ clientId, tasks }) => {
      pipe.hset("platform:profile:tasks", clientId, JSON.stringify(tasks));
    });
    await pipe.exec();
  } catch (error) {
    console.error("Ошибка при пакетной записи в Redis:", error);
    throw error;
  }
}

async function processCsvFile(filePath) {
  let batch = [];
  let batchCounter = 0;
  let processedCount = 0;
  let batchPromises = [];

  return new Promise((resolve, reject) => {
    fs.createReadStream(filePath, { encoding: "utf-8" })
      .pipe(csv())
      .on("data", (record) => {
        const clientId = record.client_id;
        if (!clientId) {
          console.warn("Обнаружена запись без client_id:", record);
          return;
        }

        const tasks = Object.entries(record)
          .slice(1)
          .filter(([, value]) => value === "True")
          .map(([key]) => key);

        batch.push({ clientId, tasks });

        if (batch.length >= config.batchSize) {
          const currentBatch = batch;
          batch = [];
          batchCounter++;

          const batchPromise = processBatch(currentBatch)
            .then(() => {
              processedCount += currentBatch.length;
              console.log(`Обработано ${processedCount} записей`);
            })
            .catch(reject);

          batchPromises.push(batchPromise);

          // Ограничиваем количество параллельных батчей
          if (batchPromises.length >= config.maxParallelBatches) {
            Promise.all(batchPromises).then(() => {
              batchPromises = [];
            });
          }
        }
      })
      .on("error", reject)
      .on("end", async () => {
        try {
          // Обрабатываем последний неполный батч
          if (batch.length > 0) {
            await processBatch(batch);
            processedCount += batch.length;
          }

          // Ждём завершения всех батчей
          await Promise.all(batchPromises);

          console.log(`Всего обработано ${processedCount} записей`);
          await renameProcessedFile(filePath);
          await safeDeleteFile(filePath); // Удаляем оригинальный файл
          resolve();
        } catch (error) {
          reject(error);
        }
      });
  });
}

// Запуск по расписанию
const job = new CronJob(
  "0 1 * * *", // Каждый день в 1:00
  async () => {
    try {
      console.log("Начало обработки файла");
      await processCsvFile(config.inputFile);
      console.log("Обработка завершена успешно");
    } catch (error) {
      console.error("Критическая ошибка при обработке:", error);
    }
  },
  null,
  true
);

// Для ручного запуска при разработке
if (require.main === module) {
  (async () => {
    await processCsvFile(config.inputFile);
    process.exit(0);
  })();
}
