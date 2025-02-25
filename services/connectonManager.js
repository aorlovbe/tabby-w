const amqp = require('amqp-connection-manager');
const log = require('./bunyan').log;
const settings = require('../settings')
let connection = null;

function createConnection() {
    if(connection) return connection;
    let urls = settings.brokerConnection;

    urls = urls.split(',');
    if(urls.length <= 0) {
        log.error('Broker connection URL is not defined');
        process.exit(1);
    }

    let maskedUrls = urls.map(url => { return url.replace(/:\/\/.*@/g, '://***:***@') })

    log.info('Connecting to', JSON.stringify(maskedUrls));

    connection = amqp.connect(urls);
    connection.on('connect', function() {
        log.warn('Connection to broker established successfully');
    });

    connection.on('disconnect', function(err) {
        log.error('Failed to connect to broker:', err);
        log.error('Reconnecting to broker...');
    });
    return connection;
}

async function close() {
    if(connection) await connection.close();
}

exports.createConnection = createConnection;
exports.close = close;