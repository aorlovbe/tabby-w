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

// was 1681678800000

// select (rowNumberInAllBlocks()+1+{{lastnum}}) as num
// and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc
let requests = [{
    "filename" : "SIGNUP",
    "headers" : '"num";"profile_id";"player_id";"game_id";"datetime";"channel";"exported";"imported"',
    "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select profile_id, player_id, 27 as game_id,\n" +
        "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
        "        gifts[1] as channel,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
        "from beeline.birthday where page = 'signup'\n" +
        "and timestamp > {{from}} and timestamp <= {{to}}  order by timestamp asc)"
    },
    {
        "filename" : "SIGNIN",
        "headers": '"num";"profile_id";"game_id";"datetime";"channel";"exported";"imported"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       27 as game_id,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "        gifts[1] as channel,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
            "from beeline.birthday where page in ('signin', 'signup')\n" +
            "and timestamp > {{from}} and timestamp <= {{to}}  order by timestamp asc)"
    },
    {
        "filename" : "ATTEMPTS",
        "headers" : '"num";"profile_id";"game_id";"match";"started";"finished";"points";"round";"reward"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "                   profile_id,\n" +
            "                   27 as game_id,\n" +
            "                   gifts[2] as match,\n" +
            "                   additional as started,\n" +
            "                   formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as finished,\n" +
            "                    gifts[1] as points,\n" +
            "                    gifts[3] as round,\n" +
            "                    gifts[4] as reward\n" +
            "            from beeline.birthday where page = 'level' and status = 'result'\n" +
            "            and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "ACTIVATIONS",
        "headers" : '"num";"profile_id";"game_id";"reward";"datetime";"type"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       CASE\n" +
            "        WHEN profile_id = '' THEN additional\n" +
            "        ELSE profile_id\n" +
            "        END AS profile_id,\n" +
            "       27 as game_id,\n" +
            "       CASE\n" +
            "        WHEN page = 'services' THEN context\n" +
            "        WHEN page = 'webhooks' THEN replaceAll(JSONExtractRaw(context, 'id'), '\"', '')\n" +
            "        ELSE details\n" +
            "        END AS reward,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "    CASE\n" +
            "        when page = 'webhooks' and details = 'activate-check' THEN 'look_up'\n" +
            "            when page = 'webhooks' and details = 'activate-click' THEN 'click'\n" +
            "       else 'purchased' END as type\n" +
            "from beeline.birthday where (page = 'webhooks' and details = 'activate-click') or (page = 'webhooks' and details = 'activate-check') or (page = 'services' and status = 'confirmed') or (page = 'presents' and status = 'present-purchased')\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "REWARDS",
        "headers" : '"num";"profile_id";"game_id";"match";"reward";"round";"datetime";"reason";"task"',
        "request" : "select num, profile_id, game_id_ as game_id, match, reward, round, datetime, reason, task from (\n" +
            "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       27 as game_id_,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'session')) as match,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'level')) as round,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       --trim(BOTH '\"' FROM JSONExtractRaw(context, 'reason')) as reason,\n" +
            "       CASE\n" +
            "        when round = '31' THEN 'super'\n" +
            "        when round = ''  THEN 'welcome'\n" +
            "       else 'game' END as reason,\n" +
            "       replaceAll(trim(BOTH '\"' FROM JSONExtractRaw(context, 'promocode')),'\u0000','') as task\n" +
            "from beeline.rewards where status = 'created' and game_id = 'birthday' and profile_id <> ''\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} and profile_id <> '' order by timestamp asc))"
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
    redis.hgetall('platform:reports:ranges-birthday', function (err, ranges){
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
            .hset('platform:reports:ranges-birthday', 'to', to)
            .hset('platform:reports:ranges-birthday', 'from', from)
            .exec(function (){
                log.warn('birthday range dates are updated:', from, to)
            })

    })
});

job.start();


function createReport(request, date,ch, from, to, callback){
    //let templated = request.request.replace('{{from}}', from).replace('{{to}}',to);
    //text.replace(new RegExp(toreplace, 'g'), entities[i][1])
    let templated = request.request.replace(new RegExp('{{from}}', 'g'), from).replace(new RegExp('{{to}}', 'g'), to);

    redis.hget('platform:reports:ranges-birthday', request.filename, function (err, last_num){
        log.warn('Getting end point for',request.filename)
        let lastnum = 0;
        if (!err && last_num !== null) lastnum = last_num;

        let lastnum_templated = templated.replace('{{lastnum}}', lastnum);

        const stream = ch.query(lastnum_templated)
        // This opens up the writeable stream to `output`
        const output = Path.resolve(__dirname, '../ftp/upload_birthday', request.filename+'_'+date +'.csv');
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
                .hincrby('platform:reports:ranges-birthday', request.filename, stream.supplemental.rows)
                .exec(function (){
                    log.warn('Range dates are updated:', from, to)
                })

            callback(request.filename);
        })
    })
}