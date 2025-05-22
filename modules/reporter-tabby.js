const ClickHouse = require("@apla/clickhouse");
let fs = require("fs");
let moment = require("moment");
const timeZone = require("moment-timezone");
const Path = require("path");
let log = require("../services/bunyan").log;
const settings = require("../settings");
let redis = require("../services/redis").redisclient;
process.env.TZ = "Europe/Moscow";
let schedule = "00 30 * * * *"; // Every hour
var CronJob = require("cron").CronJob;

let requests = [
  {
    filename: "SIGNUP",
    headers: '"num";"client_id";"datetime"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select JSONExtractString(context, 'player_id') AS client_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
      "from tabby.tabby where details = 'signup' and page = 'webhooks' and status = 'webhook' and client_id != ''\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "SIGNIN",
    headers: '"num";"client_id";"datetime"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       JSONExtractString(context, 'player_id') AS client_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
      "from tabby.tabby where details = 'signin' and page = 'webhooks' and status = 'webhook' and client_id != ''\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "ATTEMPT",
    headers: '"num";"client_id";"started";"reward"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       JSONExtractString(context, 'player_id') AS client_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started,\n" +
      "       JSONExtractString(context, 'prize') AS reward\n" +
      "from tabby.tabby where details = 'spin' and page = 'webhooks' and status = 'webhook' and client_id != ''\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "CLICKS",
    headers:
      '"num";"client_id";event;element_name;page;task_id;prize_id;"datetime"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       player_id AS client_id,\n" +
      "       details AS event,\n" +
      "       JSONExtractString(context, 'element_name') AS element_name,\n" +
      "       JSONExtractString(context, 'page') AS Page,\n" +
      "       JSONExtractString(context, 'task_id') AS task_id,\n" +
      "       JSONExtractString(context, 'prize_id') AS prize_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
      "from tabby.tabby where details = 'click' and page = 'webhooks' and status = 'webhook' and client_id != ''\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
];

let job = new CronJob(schedule, function () {
  const ch = new ClickHouse({
    host: "94.131.83.209",
    port: "8123",
    user: "accelera",
    password: "vcmR9PvEtF8O36pbic3nt",
    queryOptions: {
      database: "tabby",
    },
  });

  //Getting date ranges
  redis.hgetall("platform:reports:ranges-tabby", function (err, ranges) {
    let from;
    let to = Math.floor(new Date() - 300000);

    ranges === null || ranges === undefined
      ? (from = 1746997200000)
      : (from = ranges.to);

    let date = moment(timeZone.tz("Europe/Moscow")).format("YYYYMMDD_HHmmss");
    requests.map((request, index) => {
      setTimeout(() => {
        createReport(request, date, ch, from, to, function (done) {
          log.warn("REPORTER: Done with:", done);
        });
      }, 30000 * index); //Each 30 seconds
    });

    redis
      .multi()
      .hset("platform:reports:ranges-tabby", "to", to)
      .hset("platform:reports:ranges-tabby", "from", from)
      .exec(function () {
        log.warn("tabby range dates are updated:", from, to);
      });
  });
});

job.start();

function createReport(request, date, ch, from, to, callback) {
  //let templated = request.request.replace('{{from}}', from).replace('{{to}}',to);
  //text.replace(new RegExp(toreplace, 'g'), entities[i][1])
  let templated = request.request
    .replace(new RegExp("{{from}}", "g"), from)
    .replace(new RegExp("{{to}}", "g"), to);

  redis.hget(
    "platform:reports:ranges-tabby",
    request.filename,
    function (err, last_num) {
      log.warn("Getting end point for", request.filename);
      let lastnum = 0;
      if (!err && last_num !== null) lastnum = last_num;

      let lastnum_templated = templated.replace("{{lastnum}}", lastnum);

      const stream = ch.query(lastnum_templated);
      // This opens up the writeable stream to `output`
      const output = Path.resolve(
        __dirname,
        "../ftp/upload_to_tabby",
        request.filename + "_" + date + ".csv"
      );
      let writeStream = fs.createWriteStream(output);

      stream.on("metadata", (columns) => {
        writeStream.write(request.headers + "\n");
      });

      stream.on("data", (row) =>
        writeStream.write(row.map((i) => `"${i}"`).join(";") + "\n")
      );

      stream.on("error", (err) => {
        /* handler error */
      });

      stream.on("end", () => {
        log.warn(
          "REPORTER: Finished with report:",
          request.filename,
          from,
          to,
          "/ rows:",
          stream.supplemental.rows
        );
        writeStream.write("endfile" + "\n");
        writeStream.end();

        redis
          .multi()
          .hincrby(
            "platform:reports:ranges-tabby",
            request.filename,
            stream.supplemental.rows
          )
          .exec(function () {
            log.warn("Range dates are updated:", from, to);
          });

        callback(request.filename);
      });
    }
  );
}
