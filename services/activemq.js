var stompit             = require('stompit');
const log               = require('./bunyan').log;

function JmsClient() {

    var servers, reconnectOptions, connections, address;
    servers = [{
        'host': "84.201.146.174",
        'port': 61613,
        'connectHeaders': {
            'login': "admin",
            'passcode': "yCdGz4BJkhfkA6b4",
            'host': "84.201.146.174",
            'heart-beat': '10000,10000'
        }
    }];
    reconnectOptions = {
        maxReconnectAttempts: 10,
        maxAttempts: 10
    };
    connections = new stompit.ConnectFailover(servers, reconnectOptions);

    connections.on('connecting', function (connector) {

        address = connector.serverProperties.remoteAddress.transportPath;

        //log.info('   [i] Connecting to ActiveMQ: ' + address);
    });

    connections.on('connect', function (connector) {

        address = connector.serverProperties.remoteAddress.transportPath;

        //log.debug('   [i] Connected to ActiveMQ: ' + address);
    });

    connections.on('error', function (error) {

        address = error.connectArgs.host + ':' + error.connectArgs.port;

        log.error('ActiveMQ connection error: ' + address + ': ' + error.message);
    });

    this.channel = new stompit.Channel(connections, {
        'alwaysConnected': true
    });

}

JmsClient.prototype.subscribe = function (headers, callback) {
    return this.channel.subscribe(headers, callback);
};

JmsClient.prototype.send = function (headers, body, callback) {
    return this.channel.send(headers, body, callback);
};

JmsClient.prototype.ack = function (message, callback) {
    return this.channel.ack(message, callback);
};

exports.JmsClient = function (host, port, user, password) {
    return new JmsClient(host, port, user, password);
};