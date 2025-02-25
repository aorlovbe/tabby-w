const settings = require("../settings");
const {log} = require("../services/bunyan");
const producer = require("../services/producer");
let activemq = require('../services/activemq').JmsClient();

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer for drawlinks.com ActiveMQ */
activemq.subscribe({'destination': 'drawlinks/webhooks', 'ack': 'client-individual'}, function (error, message) {
    producer.createProducer(settings.instance).then(function (){
        message.readString('utf8', function (error, string) {
            log.debug('Got new webhook message:', string);
            let webhook = JSON.parse(string);

            try {
                producer.publishTrigger(webhook.id, webhook.event, webhook.context).then(function (){
                    log.info('Message was published to RabbitMQ triggers')
                });

            } catch (e) {
                log.error(e)
            }

        })
    })

    activemq.ack(message);
})