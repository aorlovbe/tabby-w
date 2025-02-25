const log               = require('./bunyan').log;
var _                   = require('lodash');
var redis               = require('./redis').redisclient_bulk;
var clickhouse          = require('./clickhouse');
let moment              = require('moment');

function create(table, interval, bulk) {
    this.init(table, interval, bulk);
    //interval = 20;  //rechecking every N seconds
    //bulk = 2; //2 messages in a bulk
}

create.prototype.init = function (table, interval, bulk) {
    log.info('Setting up Clickhouse bulk loader, interval / bulk:',interval + ' sec.', "/", bulk  + " msg.,", "table:", table);

    setInterval( function() {
        redis.LRANGE("platform:clickhouse:"+table,0, bulk-1, function (err, batch) {
            if (_.size(batch) !== 0) {
                log.info('Got messages for Clickhouse bulk ('+table+'):', _.size(batch));

                clickhouse.insert(batch, table, function (err, result) {
                    if (!err) {
                        redis.LTRIM('platform:clickhouse:' + table,_.size(batch),-1, function (err, result) {});
                    } else {
                        log.error('Error while inserting batch into Clickhouse, batch will be stored into :failed redis key');

                        failed(table, bulk, function (callback) {});
                    }
                    batch = null;
                });
            }
        });
    }, interval*1000 );
};

function store(table, data, callback){
    redis.RPUSH("platform:clickhouse:"+table, data, function (err) {
        if (err) {
            log.error('Cannot store row into the Redis Clickhouse batch:', table, data, err);
            callback(true);
        } else {
            callback();
        }
    })
}

function fail(method, data, callback){
    let timestamp = moment(new Date()).format('YYYY-MM-DD');

    redis.RPUSH("platform:requests:"+method+":failed:"+timestamp, data, function (err) {
        if (err) {
            log.error('Cannot store rows into the failed requests redis key', method, err);
            callback(true);
        } else {
            log.debug('Stored as failed requests key:', method, data, timestamp);
            callback();
        }
    })
}

function failed(table, bulk, callback){
    let timestamp = moment(new Date()).format('YYYY-MM-DD');

    redis.LRANGE("platform:clickhouse:"+table,0, bulk-1, function (err, batch) {
        if (!err && _.size(batch) !== 0) {
            redis.RPUSH("platform:clickhouse:"+table+":failed:"+timestamp, batch, function (err) {
                if (err) {
                    log.error('Cannot store rows into the failed redis key', err);
                    callback();
                } else {
                    log.debug('Stored as failed batch messages:', batch, timestamp);
                    redis.LTRIM('platform:clickhouse:' + table,_.size(batch),-1, function (err, result) {});
                    callback();
                }
            })
        }
    })
}

function torepeat(method, data, callback){
    let timestamp = moment(new Date()).format('YYYY-MM-DD');

    redis.RPUSH("platform:accelera:torepeat:"+method+":"+timestamp, data, function (err) {
        if (err) {
            log.error('Cannot store rows into the Accelera API redis key', err);
            callback();
        } else {
            log.error('Stored as Accelera API repeat messages:', data, timestamp);
            callback();
        }
    })
}

exports.create = create;
exports.store = store;
exports.fail = fail;
exports.torepeat = torepeat;