const axios = require("axios");
const log = require("../services/bunyan").log;
const redis = require('../services/redis').redisclient;
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

setInterval(() => {
    reload();
}, 60000*10); //each 10 min

function reload(){
    axios({
        method: 'POST',
        url: "https://beeline.accelera.ai/flow/login",
        headers: { 'Content-type' : 'application/json' },
        data: {
            "username" : 'readonly',
            "password" : 'readonly'
        },
        timeout: 30000
    }).then(done => {
        log.warn('Authorized to Accelera',done.headers);

        axios({
            method: 'GET',
            url: "https://beeline.accelera.ai/flow/api/v1/coupons/list",
            headers : {'Cookie' : done.headers['set-cookie']},
            timeout: 30000
        }).then(response => {
            log.warn('Got Accelera coupons info:', response.data.coupons);

            redis.hset('platform:accelera:coupons:info', 'coupons', JSON.stringify(response.data.coupons), function (err){
                if (err) {
                    log.error('Failed to store Accelera coupons');
                }
            })

        }).catch(err => {
            log.error('Failed to get Beeline payment auth token', err.data);
        });


    }).catch(err => {
        log.error('Failed to get Accelera authorization:', err.code);

    });
}

reload();