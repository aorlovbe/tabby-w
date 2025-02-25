let bulk = require('../services/bulk');
let log = require('../services/bunyan').log;

class Bulk {
    static store (table, data, callback) {
        bulk.store(table, JSON.stringify(data), function (err) {
            if (err) {
                log.error('Error while storing webhooks messages for Clickhouse bulk:', err);
                callback(true);
            } else {
                //log.info('Stored to', table);
                callback();
            }
        });
    }

    static fail (method, data, callback) {
        bulk.fail(method, JSON.stringify(data), function (err) {
            if (err) {
                log.error('Error while storing failed messages in Redis:', err);
                callback(true);
            } else {
                callback();
            }
        });
    }
}

module.exports = Bulk;