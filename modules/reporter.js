const ClickHouse = require('@apla/clickhouse');
let fs = require('fs');
let moment = require('moment');
const timeZone = require('moment-timezone');
const Path = require("path");
let log = require('../services/bunyan').log;
const settings = require("../settings");
let redis       = require('../services/redis').redisclient;
process.env.TZ = 'Europe/Moscow';
let schedule = '00 30 * * * *'; // Every hour
var CronJob = require('cron').CronJob;

// was 1654977600000 - 1655111100000

let requests = [{
    "filename" : "SIGNUP",
    "headers" : '"num";"profile_id";"player_id";"game_id";"datetime";"channel";"exported";"imported"',
    "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select profile_id, player_id, '3' as game_id,\n" +
        "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
        "        gifts[1] as channel,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
        "from beeline.rock_paper_scissors where page = 'signup'\n" +
        "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
},
    {
        "filename" : "SIGNIN",
        "headers": '"num";"profile_id";"game_id";"datetime";"channel";"exported";"imported"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '3' as game_id,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "        gifts[1] as channel,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
            "from beeline.rock_paper_scissors where page in ('signin', 'signup')\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "TASKS",
        "headers": '"num";"profile_id";"game_id";"task_id";"task";"datetime";"status"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '3' as game_id,\n" +
            "       splitByChar('-', name)[2] as task_id,\n" +
            "       name as task,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       CASE\n" +
            "           when replaceAll(JSONExtractRaw(context, 'status'), '\"', '') = 'active' and status = 'created' then 'created'\n" +
            "           when replaceAll(JSONExtractRaw(context, 'status'), '\"', '') = 'active' and status = 'modified' then 'clicked'\n" +
            "           when replaceAll(JSONExtractRaw(context, 'status'), '\"', '') = 'completed' then 'completed'\n" +
            "       END as status_\n" +
            "from beeline.tasks where status in ('created', 'modified') \n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "PURCHASES",
        "headers" : '"num";"profile_id";"game_id";"purchase";"pack";"datetime";"price";"value"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "      '3' as game_id,\n" +
            "       '1' as purchase,\n" +
            "       --new productId instead of pack ID\n" +
            "       gifts[4] as pack,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "        toInt16(gifts[3]) as price,\n" +
            "        toInt16(gifts[1]) as value\n" +
            "from beeline.rock_paper_scissors where status = 'purchased'\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "MATCHES",
        "headers" : '"num";"profile_id";"game_id";"match";"started";"finished";"mode";"presences";"result";"avatar";"place";"points"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '3' as game_id,\n" +
            "       match,\n" +
            "       started,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as finished,\n" +
            "       'multiplayer' as mode,\n" +
            "        presences,\n" +
            "       result,\n" +
            "       avatar,\n" +
            "       position as place,\n" +
            "       JSONExtractRaw(rewards, 'points') as points\n" +
            "from beeline.matches table_1\n" +
            "LEFT OUTER JOIN\n" +
            "(select match, formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started, trim(BOTH '[]' FROM replaceAll(profile_id, '\"', '')) as presences from beeline.matches where status = 'started' and round = 1 and standoff = 0 order by timestamp desc) table_2\n" +
            "on table_1.match = table_2.match\n" +
            "where table_1.status = 'completed' and table_1.timestamp > {{from}} and table_1.timestamp <= {{to}} order by table_1.timestamp asc)"
    },
    {
        "filename" : "ROUNDS",
        "headers" : '"num";"profile_id";"game_id";"round";"match";"standoff";"figure";"result";"datetime"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '3' as game_id,\n" +
            "       round,\n" +
            "       match,\n" +
            "       standoff,\n" +
            "       figure,\n" +
            "       result,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
            "from beeline.matches where status = 'completed'\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "COUNTERS",
        "headers" : '"num";"profile_id";"game_id";"counter";"type";"reason";"reason_id";"balance";"balance_after";"balance_before";"value";"datetime"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '3' as game_id,\n" +
            "       'tries' as counter,\n" +
            "       CASE\n" +
            "        when  toInt16(gifts[1]) < 0  then 'expense'\n" +
            "        when  toInt16(gifts[1]) > 0  then 'income'\n" +
            "    END as type,\n" +
            "       CASE\n" +
            "        when toInt16(gifts[1]) < 0  then 'match'\n" +
            "        when toInt16(gifts[1]) > 0  then 'purchase'\n" +
            "    END as reason,\n" +
            "    CASE\n" +
            "        when gifts[3] <> ''  then gifts[2]\n" +
            "        when gifts[3] = '' and reason <>  'purchase'  then 'node-1'\n" +
            "        when gifts[3] = '' and reason =  'purchase'  then 'packs'\n" +
            "    END as reason_id,\n" +
            "       gifts[2] as balance,\n" +
            "       gifts[2] as balance_after,\n" +
            "       toInt16OrZero(gifts[2])-toInt16OrZero(gifts[1]) as balance_before,\n" +
            "       gifts[1] as value,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
            "from beeline.rock_paper_scissors\n" +
            "where (page in 'counters' and details = 'tries' and status in ('created', 'modified'))\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "REWARDS",
        "headers" : '"num";"profile_id";"game_id";"reward_id";"match";"reward";"category";"datetime";"reason";"coupon"',
        "request" : "select num, profile_id, game_id_ AS game_id, reward_id, match, reward, category, datetime, reason, coupon from (select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '3' as game_id_,\n" +
            "       name as reward_id,\n" +
            "       '' as match,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
            "       'beeline' as category,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       'rating' as reason,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'promocode')) as coupon\n"+
            "from beeline.rewards where status = 'created' and game_id = 'rock-paper-scissors' \n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc))"
    },
    {
        "filename" : "RULES_ACCEPTED",
        "headers" : '"num";"profile_id";"player_id";"game_id";"datetime";"channel";"exported";"imported"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, profile_id,player_id, game_id,datetime,channel,exported, imported\n" +
            "       from (select profile_id, player_id, 3 as game_id,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "        gifts[1] as channel,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
            "       from beeline.rock_paper_scissors where page = 'signup' and player_id <> ''\n" +
            "       and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc) t1 left join\n" +
            "    (select player_id from beeline.rock_paper_scissors where page = 'rules') t2\n" +
            "    on t1.player_id = t2.player_id where ((t2.player_id <> '' and channel = 'mobile') or channel = 'web')"
    }]


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
    redis.hgetall('platform:reports:ranges', function (err, ranges){
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
            .hset('platform:reports:ranges', 'to', to)
            .hset('platform:reports:ranges', 'from', from)
            .exec(function (){
                log.warn('Range dates are updated:', from, to)
            })

    })
});

job.start();


function createReport(request, date,ch, from, to, callback){
    let templated = request.request.replace('{{from}}', from).replace('{{to}}',to);

    redis.hget('platform:reports:ranges', request.filename, function (err, last_num) {
        log.warn('Getting end point for', request.filename)
        let lastnum = 0;
        if (!err && last_num !== null) lastnum = last_num;

        let lastnum_templated = templated.replace('{{lastnum}}', lastnum);

        const stream = ch.query(lastnum_templated)
        // This opens up the writeable stream to `output`
        const output = Path.resolve(__dirname, '../ftp/upload', request.filename+'_'+date +'.csv');
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
                .hincrby('platform:reports:ranges', request.filename, stream.supplemental.rows)
                .exec(function (){
                    log.warn('Range dates are updated:', from, to)
                })

            callback(request.filename);
        })

    });
}