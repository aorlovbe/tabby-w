const log = require('./bunyan').log;
const connectionManager = require('./connectonManager');
const settings = require('../settings')
const EXCHANGE_NAME = settings.triggersExchange;
const GAME_EXCHANGE_NAME = settings.gamesExchange;

let channelWrapper = null;
let receivedEvents = 0;
let sendedEvents = 0;
let gamesReceivedEvents = 0;
let gamesSendedEvents = 0;

setInterval(() => {
    if (receivedEvents > 0) log.info(`[info] Triggers statistic, received: ${receivedEvents} | sent: ${sendedEvents}`);
    if (receivedEvents > 0) log.info(`[info] Games events statistic, received: ${gamesReceivedEvents} | sent: ${gamesSendedEvents}`);
    receivedEvents = 0;
    sendedEvents = 0;
    gamesReceivedEvents = 0;
    gamesSendedEvents = 0;
}, 30000);


function createProducer() {
    return new Promise((resolve, reject) => {
        if(channelWrapper) {
            resolve(channelWrapper);
            return;
        }

        let connection = connectionManager.createConnection();
        channelWrapper = connection.createChannel({
            json: true,
            setup: function (channel) {
                return Promise.all([
                    channel.assertExchange(EXCHANGE_NAME, 'x-consistent-hash', { durable: true }),
                    channel.assertExchange(GAME_EXCHANGE_NAME, 'direct', { durable: true }),
                    channel.assertQueue(settings.instance, { durable: true }),
                    channel.bindQueue(settings.instance, GAME_EXCHANGE_NAME, settings.instance)
                ]);
            }
        });

        channelWrapper.on('connect', () => {
            log.info('Broker event channel created successfully');
            resolve(channelWrapper);
        });

        channelWrapper.on('error', (err) => {
            log.error('Failed to create broker channel:', err);
            reject(err);
        });
    })
}

async function publishTrigger(id, eventName, context, flowId = null) {
    if(!channelWrapper) await createProducer();
    receivedEvents++;
    let event = {
        id: id,
        event: eventName,
        context: context
    }
    if(flowId) event.flowId = flowId;
    await channelWrapper._channel.publish(EXCHANGE_NAME, ''+id, Buffer.from(JSON.stringify(event)), { priority: 1 });
    sendedEvents++;
}

async function publishGameEvent(instance, event) {
    if(!channelWrapper) await createProducer();
    gamesReceivedEvents++;
    await channelWrapper._channel.publish(GAME_EXCHANGE_NAME, instance, Buffer.from(JSON.stringify(event)), { priority: 1 });
    gamesSendedEvents++;
}

exports.createProducer =  createProducer;
exports.publishTrigger = publishTrigger;
exports.publishGameEvent = publishGameEvent;