const redis = require("../services/redis").redisclient;
const csv = require("csv-parser");
const fs = require("fs");
const path = require("path");
const { promisify } = require("util");
const CronJob = require("cron").CronJob;
//123
// Конфигурация
const BATCH_SIZE = 50;
const FOLDER_PATH = "ftp/download_from_tabby/";
const CRON_SCHEDULE = "0 * * * *"; // Каждый час
const REDIS_KEY = "platform:profile:tasks";

// Увеличиваем лимиты памяти
process.env.UV_THREADPOOL_SIZE = 16; // Увеличиваем пул потоков
process.env.NODE_OPTIONS = "--max-old-space-size=8192"; // 8GB

// Promisify функции
const rename = promisify(fs.rename);
const readdir = promisify(fs.readdir);
const hSet = promisify(redis.hset).bind(redis);

async function safeProcessFiles() {
  try {
    console.log("Начало обработки файлов...");
    const files = await readdir(FOLDER_PATH);

    for (const filename of files) {
      if (shouldProcessFile(filename)) {
        await processSingleFile(filename);
      }
    }
  } catch (error) {
    console.error("Критическая ошибка:", error);
  }
}

function shouldProcessFile(filename) {
  return (
    filename.startsWith("ready_customer_task_eligibility") &&
    !filename.startsWith("parsed_") &&
    filename.endsWith(".csv")
  );
}

async function processSingleFile(filename) {
  const filePath = path.join(FOLDER_PATH, filename);
  console.log(`Обработка файла: ${filename}`);

  try {
    await new Promise((resolve, reject) => {
      const stream = fs
        .createReadStream(filePath, { encoding: "utf-8" })
        .pipe(csv())
        .on("data", async (record) => {
          try {
            await processRecord(record);
          } catch (error) {
            stream.destroy(error);
          }
        })
        .on("end", async () => {
          await renameFile(filePath);
          resolve();
        })
        .on("error", reject);
    });
  } catch (error) {
    console.error(`Ошибка обработки файла ${filename}:`, error);
  }
}

async function processRecord(record) {
  const tasks = Object.entries(record)
    .slice(1)
    .filter(([, value]) => value === "True")
    .map(([key]) => key);

  await hSet(REDIS_KEY, record.client_id, JSON.stringify(tasks));
}

async function renameFile(filePath) {
  const newFilename = "parsed_" + path.basename(filePath);
  const newPath = path.join(path.dirname(filePath), newFilename);

  try {
    await rename(filePath, newPath);
    console.log(`Файл переименован: ${newFilename}`);
  } catch (error) {
    console.error(`Ошибка переименования ${path.basename(filePath)}:`, error);
  }
}

// Запуск с обработкой ошибок
try {
  const job = new CronJob(CRON_SCHEDULE, () => {
    safeProcessFiles().catch(console.error);
  });

  job.start();
  safeProcessFiles().catch(console.error); // Первый запуск

  console.log("Скрипт успешно запущен. Расписание:", CRON_SCHEDULE);
} catch (e) {
  console.error("Ошибка запуска скрипта:", e);
  process.exit(1);
}
