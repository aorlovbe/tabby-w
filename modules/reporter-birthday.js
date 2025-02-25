const ClickHouse = require('@apla/clickhouse');
let fs = require('fs');
let moment = require('moment');
const timeZone = require('moment-timezone');
const Path = require("path");
let log = require('../services/bunyan').log;
const settings = require("../settings");
let redis       = require('../services/redis').redisclient;
process.env.TZ = 'Europe/Moscow';
let schedule = '00 40 * * * *'; // Every hour
var CronJob = require('cron').CronJob;

// was 1663102800000

let requests = [{
    "filename" : "SIGNUP",
    "headers" : '"num";"profile_id";"player_id";"game_id";"datetime";"channel";"exported";"imported"',
    "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select profile_id, player_id, '5' as game_id,\n" +
        "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
        "        gifts[1] as channel,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
        "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
        "from beeline.birthday where page = 'signup'\n" +
        "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "SIGNIN",
        "headers": '"num";"profile_id";"game_id";"datetime";"channel";"exported";"imported"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '5' as game_id,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "        gifts[1] as channel,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as exported,\n" +
            "       formatDateTime(toDateTime(now()), '%Y%m%d_%H%M%S') as imported\n" +
            "from beeline.birthday where page in ('signin', 'signup')\n" +
            "and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
    {
        "filename" : "REWARDS",
        "headers" : '"num";"profile_id";"game_id";"match";"reward";"round";"datetime";"reason";"task"',
        "request" : "select num, profile_id, game_id_, match, reward, round, datetime, reason, task from (select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       profile_id,\n" +
            "       '5' as game_id_,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'match')) as match,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'level')) as round,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       trim(BOTH '\"' FROM JSONExtractRaw(context, 'reason')) as reason,\n" +
            "       JSONExtractRaw(context, 'task') as task\n"+
            "from beeline.rewards where status = 'created' and game_id = 'birthday' \n" +
            "and timestamp > {{from}} and timestamp <= {{to}} and profile_id <> '' order by timestamp asc))"
    },
/*    {
        "filename" : "ATTEMPT",
        "headers" : '"num";"profile_id";"game_id";"match";"started";"finished";"points";"round";"reward"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, profile_id, game_id, match, started, finished, points, round, reward from (select\n" +
            "       profile_id,\n" +
            "       5 as game_id,\n" +
            "       context as match,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as started\n" +
            "from beeline.birthday where page = 'sessions' and status = 'created' and timestamp > {{from}} and timestamp <= {{to}}) table_1 LEFT OUTER JOIN (select\n" +
            "                        details as match,\n" +
            "                        trim(BOTH '\"' FROM JSONExtractRaw(context, 'id')) as reward,\n" +
            "                        1 as points,\n" +
            "                        JSONExtractRaw(additional, 'level') as round,\n" +
            "                        formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as finished\n"+
            "from beeline.birthday where page = 'sessions' and status = 'stored' and timestamp > {{from}} and timestamp <= {{to}} order by timestamp desc) table_2\n" +
            "on table_1.match = table_2.match"
    },*/
    {
        "filename" : "ACTIVATIONS",
        "headers" : '"num";"profile_id";"game_id";"reward";"datetime"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select\n" +
            "       CASE\n" +
            "        WHEN profile_id = '' THEN additional\n" +
            "        ELSE profile_id\n" +
            "        END AS profile_id,\n" +
            "       5 as game_id,\n" +
            "       context as reward,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime\n" +
            "from beeline.birthday where page = 'services' and status = 'confirmed' and timestamp > {{from}} and timestamp <= {{to}} order by timestamp asc)"
    },
/*    {
        "filename" : "TASKS",
        "headers" : '"num";"profile_id";"game_id";"task_id";"task";"datetime";"status"',
        "request" : "select (rowNumberInAllBlocks()+1+{{lastnum}}) as num, * from (select distinct\n" +
            "       profile_id,\n" +
            "       5 as game_id,\n" +
            "       '2' as task_id,\n" +
            "       'task-2' as task,\n" +
            "       formatDateTime(toDateTime(timestamp/1000), '%Y%m%d_%H%M%S') as datetime,\n" +
            "       'completed' as status\n" +
            "from beeline.rewards where trim(BOTH '\"' FROM JSONExtractRaw(context, 'reason')) = 'tasks'\n" +
            "and timestamp > {{from}} and timestamp <= {{to}}\n" +
            "order by timestamp asc)"
    }*/
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
            }, 10000*index ) //Each 10 seconds
        })

        redis.multi()
            .hset('platform:reports:ranges-birthday', 'to', to)
            .hset('platform:reports:ranges-birthday', 'from', from)
            .exec(function (){
                log.warn('Birthday range dates are updated:', from, to)
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