const log = require('./bunyan').log;
const connectionManager = require('./connectonManager');
const settings = require('../settings')
let channelWrapper = null;

function createConsumer(instance, callback) {
    let connection = connectionManager.createConnection();
    channelWrapper = connection.createChannel({
        setup: function(channel) {
            return Promise.all([
                channel.assertQueue(settings.instance, { durable: true }),
                channel.prefetch(1),
                channel.consume(settings.instance, callback)
            ]);
        }
    });

    channelWrapper.waitForConnect()
        .then(function() {
            log.info("Accelera API is ready to receive events:", settings.instance);
        });
}

function ack(msg) {
    channelWrapper.ack(msg);
}

async function close() {
    if(channelWrapper) await channelWrapper.close();
}

exports.createConsumer = createConsumer;
exports.close = close;
exports.ack = ack;