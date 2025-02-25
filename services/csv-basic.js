const es = require('event-stream');
const fs = require('fs');
const path = require('path');
const log = require('./bunyan').log;
const now = require("performance-now");

function parse(filepath, separator, callback, finished) {
    let firstRowFlag = true;
    let rows = 1;
    let firstRowArgs;
    var start = now();
    var event;
    let fileStream = fs.createReadStream(filepath);

    fileStream.on('error', (err) => {
        callback(err);
    });

    fileStream.pipe(es.split())
        .pipe(es.mapSync(function (data) {
            if (firstRowFlag) {
                firstRowArgs = data.split(separator);
                firstRowFlag = false;
            } else {
                let args = data.split(separator);
                event = {};

                for (let index in args) {
                    event[firstRowArgs[index].replace(/^"|"$/g, '')] = args[index].replace(/^"|"$/g, '');
                }
                callback(null, rows++, event);
            }
        }))
        .on('error', function (err) {
            callback(err);
            fs.rename(filepath, path.join(path.dirname(filepath),"error_" + path.basename(filepath)), function (err) {
                log.error('File', filepath, 'renamed with error prefix');
            });
        })
        .on('end', function () {
            log.debug(`File ${filepath} successfully processed, ${rows} rows / ${(now() - start).toFixed(3)} ms`);
            finished(rows);

            fs.rename(filepath, path.join(path.dirname(filepath),"done_" + path.basename(filepath)), function (err) {
                log.info('File', filepath, 'renamed with done prefix');
            });
        })
}

exports.parse = parse;