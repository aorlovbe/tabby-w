const axios = require("axios");
const settings = require("../settings");
const log = require("../services/bunyan").log;
const redis = require('../services/redis').redisclient;
const _ = require('lodash');
const producer = require('../services/producer');
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

log.debug('Token manager is started:', settings.beeline.payment_accounts);

setInterval(() => {
    healthcheck(function (err, node, stats){

    });
}, 60000*10); //each 10 min

function getToken(node, callback){
    axios({
        method: 'POST',
        url: node + ":7351/v2/console/authenticate",
        headers: { 'Content-type' : 'application/json' },
        data: {
            "username" : settings.nakama.user,
            "password" : settings.nakama.password
        },
        timeout: 30000
    }).then(response => {
        log.debug('Authorized nakama token:', node, response.data.token);
        callback(response.data.token)

    }).catch(err => {
        log.error('Failed to get Nakama token:', node, err.code);
        redis.hset('platform:nakama', node, JSON.stringify({"health" : -1}), function (){})
    });
}

function healthcheck(callback){
    let nodes = settings.nakama.nodes.split(',')
    _.forEach(nodes, function (node){
        getToken(node, function (token){
            axios({
                method: 'POST',
                url: node + ":7351/v2/console/status",
                headers: { 'Authorization' : 'Bearer '+ token },
                timeout: 30000
            }).then(response => {
                log.info('[info] Nakama node status:', node,
                    '| sessions:', response.data.nodes[0].session_count,
                    '| presences:', response.data.nodes[0].presence_count,
                    '| matches:', response.data.nodes[0].match_count,
                    '| AVG latency', response.data.nodes[0].avg_latency_ms,
                    '| health', response.data.nodes[0].health);

                let _node = {
                    "node" : node,
                    "token" : token,
                    "name": response.data.nodes[0].name,
                    "health": response.data.nodes[0].health,
                    "session_count": response.data.nodes[0].session_count,
                    "presence_count": response.data.nodes[0].presence_count,
                    "match_count": response.data.nodes[0].match_count,
                    "goroutine_count": response.data.nodes[0].goroutine_count,
                    "avg_latency_ms": response.data.nodes[0].avg_latency_ms,
                    "avg_rate_sec": response.data.nodes[0].avg_rate_sec,
                    "avg_input_kbs": response.data.nodes[0].avg_input_kbs,
                    "avg_output_kbs": response.data.nodes[0].avg_output_kbs
                };

                redis.hset('platform:nakama', node, JSON.stringify(_.cloneDeep(_node)), function (){})
                callback(false,node,response.data.nodes[0])

            }).catch(err => {
                log.error('Failed to get Nakama node status:', node, err);
                redis.hset('platform:nakama', node, JSON.stringify({"health" : -1}), function (){})
                callback(true, node, {"health" : -1});
            });
        })
    })
}

healthcheck(function (err, node, stats){

});