const ClickHouse = require("@apla/clickhouse");
let fs = require("fs");
let moment = require("moment");
const timeZone = require("moment-timezone");
const Path = require("path");
let log = require("../services/bunyan").log;
const settings = require("../settings");
let redis = require("../services/redis").redisclient;
process.env.TZ = "Europe/Moscow";
let schedule = "00 05 * * * *"; // Every hour
var CronJob = require("cron").CronJob;

// was 1681678800000

// select (rowNumberInAllBlocks()+1+{{lastnum}}) as num
// and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc
let requests = [
  {
    filename: "ATTEMPT",
    headers:
      '"num";"profile_id";"game_id";"session_id";"attempt_id";"started";"number_cell_start";"number_cell_final";"correct_path_flag";"cell_type";"reward_id"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       profile_id,\n" +
      "       50 as game_id,\n" +
      "       additional as session_id,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(details, 'attempt_id')) as attempt_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started,\n" +
      "       JSONExtractRaw(details, 'current:') as number_cell_start,\n" +
      "       JSONExtractRaw(details, 'next') as number_cell_final,\n" +
      "       JSONExtractRaw(context, 'right_path') as correct_path_flag,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(details, 'type')) as  cell_type,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'content')) as  reward_id\n" +
      "from beeline.xmas2023 where page = 'map' and status in ('step','obstacle') and correct_path_flag <> ''\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "BARRIER",
    headers:
      '"num";"profile_id";"game_id";"session_id";"attempt_id";"started";"number_cell_start";"number_cell_final";"barrier_id";"barrier_status";"used_ability"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       profile_id,\n" +
      "       50 as game_id,\n" +
      "       additional as session_id,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(details, 'attempt_id')) as attempt_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started,\n" +
      "       JSONExtractRaw(details, 'current:') as number_cell_start,\n" +
      "       JSONExtractRaw(details, 'next') as number_cell_final,\n" +
      "        trim(BOTH '\"' FROM JSONExtractRaw(details, 'id')) as barrier_id,\n" +
      "CASE\n" +
      "        WHEN status = 'passed-obstacle' THEN 'done'\n" +
      "        ELSE 'not_done'\n" +
      "        END AS barrier_status,\n" +
      "        trim(BOTH '\"' FROM JSONExtractRaw(details, 'power')) as used_ability\n" +
      "\n" +
      "from beeline.xmas2023 where page = 'map' and status in ('passed-obstacle','not-passed-obstacle')\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
];

let job = new CronJob(schedule, function () {
  const ch = new ClickHouse({
    host: "84.201.144.141",
    port: "8123",
    user: "default",
    password: "helloworld163f8c4e262f87e1bc0c1f3af2",
    queryOptions: {
      database: "beeline",
    },
  });

  //Getting date ranges
  redis.hgetall(
    "platform:reports:ranges-xmas2023-obstacles",
    function (err, ranges) {
      let from = ranges.to;
      let to = Math.floor(new Date());

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
        .hset("platform:reports:ranges-xmas2023-obstacles", "to", to)
        .hset("platform:reports:ranges-xmas2023-obstacles", "from", from)
        .exec(function () {
          log.warn("Orange range dates are updated:", from, to);
        });
    }
  );
});

job.start();

function createReport(request, date, ch, from, to, callback) {
  //let templated = request.request.replace('{{from}}', from).replace('{{to}}',to);
  //text.replace(new RegExp(toreplace, 'g'), entities[i][1])
  let templated = request.request
    .replace(new RegExp("{{from}}", "g"), from)
    .replace(new RegExp("{{to}}", "g"), to);

  redis.hget(
    "platform:reports:ranges-xmas2023-obstacles",
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
        "../ftp/upload_xmas2023",
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
            "platform:reports:ranges-xmas2023-obstacles",
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
