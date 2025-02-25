/**
 * Created by fla_ on 31.07.17.
 */

var request             = require('request');
const log               = require('./bunyan').log;
var _                   = require('lodash');
const settings = require("../settings");

var options = {
    'method': 'POST',
    'timeout' : 60000,
    'url': 'http://'+settings.clickhouse.host + ':' + settings.clickhouse.port +'/?database=' + settings.clickhouse.db,
    'headers': {
        'X-ClickHouse-User': settings.clickhouse.login,
        'X-ClickHouse-Key': settings.clickhouse.pass,
        'Content-Type' : 'application/json'
    }
};

function insert(batch, table, sent_callback) {
    options.body = "INSERT INTO "+ table.replace(/-/g, '_') + " FORMAT JSONEachRow" + " " + batch;

    request(options, function callback(error, response) {
        if (!error && response.statusCode == 200) {
            log.debug("Clickhouse games batch inserted:", _.size(batch), "/", table);
            sent_callback(false, 'ok');

        } else {
            log.error("Error while inserting Clickhouse games batch", JSON.stringify(options.body), error, response);
            sent_callback(true, JSON.stringify(options.body), error, response);
        }
    });
}

exports.insert = insert;