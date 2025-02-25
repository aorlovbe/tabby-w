module.exports = {
    telegram: {
        token : process.env.TELEGRAM_TOKEN
    },
    redis : {
        connection : process.env.REDIS_CONNECTION,
    },
    jwt: process.env.JWT_SECRET,
    sftp: {
        upload: process.env.FTP_UPLOAD,
        download: process.env.FTP_DOWNLOAD,
        host: process.env.FTP_HOST,
        login: process.env.FTP_LOGIN,
        pass: process.env.FTP_PASS,
    },
    nakama: {
        nodes: process.env.NAKAMA_NODES,
        user: process.env.USERNAME,
        password: process.env.PASSWORD
    },
    beeline : {
        sms: process.env.SMS_SERVICE,
        sender: process.env.SMS_SENDER,
        sender_all: process.env.SMS_SENDER_ALLOPERATORS,
        userid: process.env.SMS_USERID,
        pass: process.env.SMS_PASS,
        userid_all: process.env.SMS_USERID_ALLOPERATORS,
        pass_all: process.env.SMS_PASS_ALLOPERATORS,
        payments: process.env.PAYMENT_URL,
        payment_accounts: process.env.PAYMENT_ACCOUNT,
        appid: process.env.APPID,
        partner: process.env.PARTNER,
        secret: process.env.PAYMENT_SECRET,
        push: process.env.PUSH
    },
    clickhouse : {
        host: process.env.CLICKHOUSE_HOST,
        port: process.env.CLICKHOUSE_PORT,
        db: process.env.CLICKHOUSE_DATABASE,
        login: process.env.CLICKHOUSE_LOGIN,
        pass: process.env.CLICKHOUSE_PASSWORD
    },
    brokerConnection: process.env.BROKER_CONNECTION,
    triggersExchange: process.env.EXCHANGE_NAME,
    gamesExchange: process.env.GAME_EXCHANGE_NAME,
    instance: process.env.API_INSTANCE,
    loglevel: process.env.LOG_LEVEL || 'info',
    server: {
        port: process.env.PORT || 8000
    }
}