const TelegramBot = require('node-telegram-bot-api');
let log = require('../services/bunyan').log;
let settings = require('../settings');
const producer = require("../services/producer");

const token = settings.telegram.token; //Accelera bot

// Create a bot that uses 'polling' to fetch new updates
const bot = new TelegramBot(token, {polling: true});

producer.createProducer(settings.instance).then(function (){

    // Listen for any kind of message. There are different kinds of messages.
    bot.on('message', (msg) => {
        const chatId = msg.chat.id;
        let sender = (msg.chat.username === undefined) ? msg.from.username : msg.chat.username;

        // send a message to the chat acknowledging receipt of their message
        //bot.sendMessage(chatId, 'Получено сообщение, @' + sender);
        log.info(msg.text);
        //checkStartMapping(msg, msg.text);
    });
    bot.on("polling_error", console.log);

    bot.on('callback_query', function (msg) {
        const chatId = msg.message.chat.id;
        const chatTitle = msg.message.chat.title;
        let sender = msg.from.username;
        // send a message to the chat acknowledging receipt of their message

        try {
            let reply = msg.data.split(',');
            // 0 - message
            log.info(msg, reply);
            //bot.sendMessage(chatId, 'Да, именно так!');

            //Pushing to accelera
            try {
                producer.publishTrigger(msg.message.chat.id, "chat", {
                    "text" : msg.data,
                    "telegramId" : msg.message.chat.id,
                    "firstName" : msg.message.chat.first_name,
                    "lastName" : msg.message.chat.last_name,
                    "userName" : msg.message.chat.user_name
                }).then(function (){
                    log.info('Message was published to RabbitMQ triggers')
                    log.info('Sent back:', JSON.stringify({
                        "id": msg.message.chat.id,
                        "event": "chat",
                        "context": {
                            "text" : msg.data,
                            "telegramId" : msg.message.chat.id,
                            "firstName" : msg.message.chat.first_name,
                            "lastName" : msg.message.chat.last_name,
                            "userName" : msg.message.chat.user_name
                        }
                    }));
                });

            } catch (e) {
                log.error(e)
            }

        } catch (e) {
            log.error('Error:', msg, e);
        }
    });


    function checkStartMapping(msg, text){
        if (text.includes('/start') === true){
            let parsed = text.split(' ');
            log.info('Start command with payload ID:', parsed);

            //Pushing to accelera
            try {
                producer.publishTrigger(parsed[1], "started", {
                    "telegramId" : msg.from.id,
                    "firstName" : msg.from.first_name,
                    "lastName" : msg.from.last_name,
                    "userName" : msg.from.user_name,
                    "profile_id" : parsed[1]
                }).then(function (){
                    log.info('Message was published to RabbitMQ triggers')
                    log.info('Sent back:', JSON.stringify({
                        "id": msg.message.chat.id,
                        "event": "chat",
                        "context": {
                            "text" : msg.data,
                            "telegramId" : msg.message.chat.id,
                            "firstName" : msg.message.chat.first_name,
                            "lastName" : msg.message.chat.last_name,
                            "userName" : msg.message.chat.user_name
                        }
                    }));
                });

            } catch (e) {
                log.error(e)
            }

        } else {
            //Pushing to accelera
            try {
                producer.publishTrigger(msg.chat.id, "chat", {
                    "telegramId" : msg.from.id,
                    "firstName" : msg.from.first_name,
                    "lastName" : msg.from.last_name,
                    "userName" : msg.from.user_name,
                    "profile_id" : parsed[1]
                }).then(function (){
                    log.info('Message was published to RabbitMQ triggers')
                    log.info('Sent back:', msg, JSON.stringify({
                        "id": msg.chat.id,
                        "event": "chat",
                        "context": {
                            "text" : text,
                            "telegramId" : msg.chat.id,
                            "firstName" : msg.chat.first_name,
                            "lastName" : msg.chat.last_name,
                            "userName" : msg.chat.user_name
                        }
                    }));
                });

            } catch (e) {
                log.error(e)
            }
        }
    }
})