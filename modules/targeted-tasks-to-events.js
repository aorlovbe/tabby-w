const csv = require('../services/csv-basic');
const _ = require('lodash');
var glob = require("glob");
const path = require('path');
const fs = require('fs');
const Promise = require('bluebird');
const log = require('../services/bunyan').log;
let bulk = require('../services/bulk');
const moment = require('moment');
const timeZone = require('moment-timezone');
const producer = require('../services/producer');
const settings = require("../settings");
const API = require('../middleware/api');

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer and Game API events consumer */
producer.createProducer(settings.instance).then(function (){
    log.info('Accelera Game API targeted tasks to event worker is created:', settings.instance);
    //Starting schedule
    start();

    setInterval(function () {
        start();
    },1000*60);
});

function events(target) {
    //m44Pbr805tqpy0z5qXWU
    csv.parse(target, ';', (err, rows, result) => {
        //Transformation function
        if(err) return log.error(err.message);

        if (result.profile_id !== '' && result.profile_id !== 'endfile')  {
            let out = {
                profile_id : result.profile_id,
                name: 'task-' + result.task_id,
                count: 1,
                load_dttm: result.load_dttm
            }
            log.warn('Processed task:', out);

            //Publish trigger
            API.publish(result.profile_id, 'task-targeted', out, function (){})

            bulk.store('rock-paper-scissors', JSON.stringify({
                timestamp : Math.floor(new Date()),
                profile_id: result.profile_id,
                game_id: 'rock-paper-scissors',
                event: 'accelera-api',
                page: "targeted-tasks-worker",
                status: "processed",
                additional: JSON.stringify(out),
                date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
            }), function () {});
        } else {
            log.info('End of file')
        }


    }, (done) => {
        log.info('Done with a file:', target);
    });
}

function start(){
    glob(path.join(__dirname, '../ftp/download', "@(TA_TASKS*)"), function (er, files) {
        if (files.length !== 0) {
            log.debug("Found tasks files:", files.length);


            Promise.each(files, function(file) {
                return events(file);
            }).then(function(result) {

            }).catch(function(err) {
                log.error('Got error while processing mission files:', err);
            });

        } else
        {
            //log.warn('Nothing to parse');
        }
    });
}