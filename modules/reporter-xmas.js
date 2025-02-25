const ClickHouse = require('@apla/clickhouse');
let fs = require('fs');
let moment = require('moment');
const timeZone = require('moment-timezone');
const Path = require("path");
let log = require('../services/bunyan').log;
const settings = require("../settings");
let redis       = require('../services/redis').redisclient;
process.env.TZ = 'Europe/Moscow';
let schedule = '00 20 * * * *'; // Every hour
var CronJob = require('cron').CronJob;

// was 1670446800000

// select (rowNumberInAllBlocks()+1+{{lastnum}}) as num
// and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc
let requests = [{
    "filename" : "SIGNUP",
    "headers" : '"num";"profile_id";"player_id";"game_id";"datetime";"channel";"exported";"imported"',
    "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select profile_id, player_id, 9 as game_id,\n" +
        "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
        "        gifts[1] as channel,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
        "from beeline.xmas where page = 'signup'\n" +
        "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "SIGNIN",
        "headers": '"num";"profile_id";"game_id";"datetime";"channel";"exported";"imported"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       9 as game_id,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "        gifts[1] as channel,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
            "from beeline.xmas where page in ('signin', 'signup')\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "REWARDS",
        "headers" : '"num";"profile_id";"game_id";"match";"reward";"datetime";"score";"coupon";"reason"',
        "request" : "select num, profile_id, game_id_ as game_id, match, reward, datetime, score, coupon, reason from (\n" +
            "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       9 as game_id_,\n" +
            "       profile_id as match,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       CASE\n" +
            "        WHEN substring(toString(reward), 1, 2) = 'r-' THEN substring(toString(reward), 3, 10)\n" +
            "        ELSE '0'\n" +
            "        END AS score,\n" +
            "       JSONExtractRaw(context, 'promocode') as coupon,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'reason')) as reason\n" +
            "from beeline.rewards where status = 'created' and game_id = 'xmas' and profile_id <> ''\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} and profile_id <> '' order by timestamp asc))"
    },
    {
        "filename" : "ATTEMPTS",
        "headers" : '"num";"profile_id";"game_id";"match";"started";"dice_value";"type_cell";"number_cell";"type_cell_final";"type_cell_final"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       9 as game_id,\n" +
            "       profile_id as match,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started,\n" +
            "        details as dice_value,\n" +
            "        gifts[2] as type_cell,\n" +
            "        gifts[1] as number_cell,\n" +
            "        gifts[4] as type_cell_final,\n" +
            "        gifts[3] as number_cell_final\n" +
            "from beeline.xmas where page = 'map' and status = 'step'\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "ACTIVATIONS",
        "headers" : '"num";"profile_id";"game_id";"reward";"datetime"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       CASE\n" +
            "        WHEN profile_id = '' THEN additional\n" +
            "        ELSE profile_id\n" +
            "        END AS profile_id,\n" +
            "       9 as game_id,\n" +
            "       CASE\n" +
            "        WHEN page = 'services' THEN context\n" +
            "        ELSE details\n" +
            "        END AS reward,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
            "from beeline.xmas where ((page = 'services' and status = 'confirmed') or (page = 'presents' and status = 'present-purchased'))\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "TASKS",
        "headers" : '"num";"profile_id";"game_id";"task_id";"task";"datetime";"status"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       9 as game_id_,\n" +
            "       splitByChar('-', name)[3] as task_id,\n" +
            "       name as task,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       CASE\n" +
            "           when replaceAll(JSONExtractRaw(context, 'status'), '\"', '') = 'active' and status = 'created' then 'created'\n" +
            "           when replaceAll(JSONExtractRaw(context, 'status'), '\"', '') = 'active' and status = 'modified' then 'clicked'\n" +
            "           when replaceAll(JSONExtractRaw(context, 'status'), '\"', '') = 'completed' then 'completed'\n" +
            "       END as status_\n" +
            "from beeline.tasks where status in ('created', 'modified') and game_id = 'xmas'\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "UPPER",
        "headers" : '"num";"profile_id";"game_id";"upper_id";"upper_dt"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       9 as game_id,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'character')) as upper_id,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as upper_dt\n" +
            "from beeline.xmas where page = 'webhooks' and details = 'character' and profile_id <> '' \n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "APP_WEB_METRICS",
        "headers" : '"num";"profile_id";"game_id";"event";"place";"reward";"link";"event_dt"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       9 as game_id,\n" +
            "       CASE\n" +
            "        WHEN details = 'activate-check' THEN 'view'\n" +
            "        WHEN details = 'activate-click' THEN 'activate'\n" +
            "        WHEN details = 'lookup' THEN 'lookup'\n" +
            "       END AS event,\n" +
            "       CASE\n" +
            "        WHEN details = 'activate-check' THEN 'rewards'\n" +
            "        WHEN details = 'activate-click' THEN 'rewards'\n" +
            "        WHEN details = 'lookup' THEN 'map'\n" +
            "       END AS place,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'link')) as link,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as event_dt\n" +
            "from beeline.xmas where page = 'webhooks' and details <> 'character' and profile_id <> ''\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    }
]


let job = new CronJob(schedule, function() {
    const ch = new ClickHouse({
        "host" : "84.201.144.141",
        "port" : "8123",
        "user" : "default",
        "password" : "helloworld163f8c4e262f87e1bc0c1f3af2",
        "queryOptions": {
            "database": "beeline"
        }
    })

    //Getting date ranges
    redis.hgetall('platform:reports:ranges-xmas', function (err, ranges){
        let from = ranges.to;
        let to = Math.floor(new Date());

        let date = moment(timeZone.tz('Europe/Moscow')).format('YYYYMMDD_HHmmss');
        requests.map((request, index) => {

            setTimeout(() => {
                createReport(request, date, ch, from, to, function (done){
                    log.warn('REPORTER: Done with:',done)
                })
            }, 30000*index ) //Each 30 seconds
        })

        redis.multi()
            .hset('platform:reports:ranges-xmas', 'to', to)
            .hset('platform:reports:ranges-xmas', 'from', from)
            .exec(function (){
                log.warn('xmas range dates are updated:', from, to)
            })

    })
});

job.start();


function createReport(request, date,ch, from, to, callback){
    //let templated = request.request.replace('{{from}}', from).replace('{{to}}',to);
    //text.replace(new RegExp(toreplace, 'g'), entities[i][1])
    let templated = request.request.replace(new RegExp('{{from}}', 'g'), from).replace(new RegExp('{{to}}', 'g'), to);

    redis.hget('platform:reports:ranges-xmas', request.filename, function (err, last_num){
        log.warn('Getting end point for',request.filename)
        let lastnum = 0;
        if (!err && last_num !== null) lastnum = last_num;

        let lastnum_templated = templated.replace('{{lastnum}}', lastnum);

        const stream = ch.query(lastnum_templated)
        // This opens up the writeable stream to `output`
        const output = Path.resolve(__dirname, '../ftp/upload_xmas', request.filename+'_'+date +'.csv');
        let writeStream = fs.createWriteStream(output);

        stream.on('metadata', (columns) => {
            writeStream.write(request.headers+'\n')
        })

        stream.on('data', (row) =>
            writeStream.write(row.map(i => `"${i}"`).join(';')+'\n'))

        stream.on('error', (err) => { /* handler error */ })

        stream.on('end', () => {
            log.warn('REPORTER: Finished with report:', request.filename, from, to, '/ rows:',stream.supplemental.rows)
            writeStream.write('endfile'+'\n')
            writeStream.end();

            redis.multi()
                .hincrby('platform:reports:ranges-xmas', request.filename, stream.supplemental.rows)
                .exec(function (){
                    log.warn('Range dates are updated:', from, to)
                })

            callback(request.filename);
        })
    })
}