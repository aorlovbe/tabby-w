const ClickHouse = require("@apla/clickhouse");
let fs = require("fs");
let moment = require("moment");
const timeZone = require("moment-timezone");
const Path = require("path");
let log = require("../services/bunyan").log;
const settings = require("../settings");
let redis = require("../services/redis").redisclient;
process.env.TZ = "Europe/Moscow";
let schedule = "00 56 * * * *"; // Every hour
var CronJob = require("cron").CronJob;

// was 1681678800000

// select (rowNumberInAllBlocks()+1+{{lastnum}}) as num
// and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc
let requests = [
  {
    filename: "SIGNUP",
    headers:
      '"num";"profile_id";"player_id";"game_id";"datetime";"channel";"exported";"imported"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select profile_id, player_id, 50 as game_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
      "        gifts[1] as channel,\n" +
      "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
      "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
      "from beeline.xmas2023 where page = 'signup'\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "SIGNIN",
    headers:
      '"num";"profile_id";"game_id";"datetime";"channel";"exported";"imported"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       profile_id,\n" +
      "       50 as game_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
      "        gifts[1] as channel,\n" +
      "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
      "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
      "from beeline.xmas2023 where page in ('signin', 'signup')\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "SESSION",
    headers:
      '"num";"profile_id";"game_id";"session_id";"started";"ability_booster_flag";"points_booster_flag";"ability_s1";"ability_s2";"ability_s3";"ability_s4";"ability_s5";"ability_s6"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       profile_id,\n" +
      "       50 as game_id,\n" +
      "       additional as session_id,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started,\n" +
      "       gifts[2] as ability_booster_flag,\n" +
      "       gifts[1] as points_booster_flag,\n" +
      "        JSONExtractRaw(details, 'magic') as ability_s1,\n" +
      "        JSONExtractRaw(details, 'protection') as ability_s2,\n" +
      "        JSONExtractRaw(details, 'intelligence') as ability_s3,\n" +
      "        JSONExtractRaw(details, 'humor') as ability_s4,\n" +
      "        JSONExtractRaw(details, 'dexterity') as ability_s5,\n" +
      "        JSONExtractRaw(details, 'strength') as ability_s6\n" +
      "from beeline.xmas2023 where page = 'map' and status = 'play'\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "REWARDS",
    headers:
      '"num";"profile_id";"game_id";"session_id";"attemt_id";"reward";"datetime";"issue_status";"coupon";"reason"',
    request:
      "select num, profile_id, game_id_ as game_id, session_id, attempt_id, reward,  datetime, issue_status, coupon, reason from (\n" +
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       profile_id,\n" +
      "       50 as game_id_,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'last_step_uuid')) as session_id,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'last_move_uuid')) as attempt_id,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
      "       CASE\n" +
      "        WHEN reward in ('t-1','t-2','t-3','t-4','t-5','t-8','t-9','t-10','t-11','m-1','m-3') and status = 'created' THEN 'created'\n" +
      "        WHEN reward in ('t-1','t-2','t-3','t-4','t-5','t-8','t-9','t-10','t-11','m-1','m-3') and status = 'modified' THEN trim(BOTH '\"' FROM JSONExtractRaw(context, 'decision'))\n" +
      "        ELSE 'null'\n" +
      "        END AS issue_status,\n" +
      "       JSONExtractRaw(context, 'promocode') as coupon,\n" +
      "       'attempt' as reason\n" +
      "from beeline.rewards where status in ('created','modified') and game_id = 'xmas2023' and profile_id <> ''\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} and profile_id <> '' order by timestamp asc))",
  },
  {
    filename: "ACTIVATIONS",
    headers: '"num";"profile_id";"player_id";"game_id";"reward";"datetime"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       CASE\n" +
      "        WHEN profile_id = '' THEN additional\n" +
      "        ELSE profile_id\n" +
      "        END AS profile_id,\n" +
      "       player_id,\n" +
      "       50 as game_id,\n" +
      "       CASE\n" +
      "        WHEN page = 'services' THEN context\n" +
      "        ELSE details\n" +
      "        END AS reward,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
      "from beeline.xmas2023 where ((page = 'services' and status = 'confirmed') or (page = 'presents' and status = 'present-purchased'))\n" +
      "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)",
  },
  {
    filename: "APP_WEB_METRICS",
    headers:
      '"num";"profile_id";"game_id";"event";"place";"reward";"link";"event_dt"',
    request:
      "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
      "       profile_id,\n" +
      "       50 as game_id,\n" +
      "       CASE\n" +
      "        WHEN details = 'activate-check' THEN 'view'\n" +
      "        WHEN details = 'activate-click' THEN 'activate'\n" +
      "        WHEN details = 'lookup' THEN 'lookup'\n" +
      "       END AS event,\n" +
      "       CASE\n" +
      "        WHEN details = 'activate-check' THEN 'prizes'\n" +
      "        WHEN details = 'activate-click' THEN 'prizes'\n" +
      "        WHEN details = 'lookup' THEN 'map'\n" +
      "       END AS place,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
      "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'link')) as link,\n" +
      "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as event_dt\n" +
      "from beeline.xmas2023 where page = 'webhooks' and event <> '' and profile_id <> ''\n" +
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
  redis.hgetall("platform:reports:ranges-xmas2023", function (err, ranges) {
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
      .hset("platform:reports:ranges-xmas2023", "to", to)
      .hset("platform:reports:ranges-xmas2023", "from", from)
      .exec(function () {
        log.warn("Orange range dates are updated:", from, to);
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
    "platform:reports:ranges-xmas2023",
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
            "platform:reports:ranges-xmas2023",
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
