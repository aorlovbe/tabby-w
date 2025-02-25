const producer = require('../services/producer');
const redis = require('../services/redis').redisclient;
const axios = require('axios');
const _ = require('lodash');
const settings = require('../settings');
const sha = require('../services/sha');
const qs = require('qs');
const log = require('../services/bunyan').log;
let moment = require('moment');
const momentTimezone = require('moment-timezone');

class Payments {
    static auth(callback) {
        redis.hget('platform:tokens', 'beeline-payments', function (err, token){
            if (err || token === null) {
                axios({
                    method: 'GET',
                    url: settings.beeline.payments + "/auth/token",
                    headers: { 'Authorization' : 'Basic '+ settings.beeline.payment_accounts },
                    timeout: 30000
                }).then(response => {
                    log.debug('Authorized (refreshed) token:', response.data.access_token);
                    callback(response.data.access_token);
                }).catch(err => {
                    log.error('Failed to process Beeline payment auth message', err);
                });
            } else {
                return callback(token)
            }
        })
    }


    static packs(callback) {
        redis.hget('platform:tokens', 'beeline-payments', function (err, token) {
            if (err) {
                log.error('Failed to get Beeline payment auth token:', err);
                return callback(true);
            } else {
                axios({
                    method: 'GET',
                    url: settings.beeline.payments + "/game/list",
                    headers: {
                        'Authorization' : 'Bearer '+ token,
                        'Content-type' : 'application/json'
                    },
                    params: {"phone" : "79880001893"},
                    timeout: 30000
                }).then(response => {
                    log.debug('Got packs:', response.data);

                    callback(response.data);
                }).catch(err => {
                    log.error('Failed to process Beeline payment packs message', err);
                    callback(true);
                });
            }
        })
    }

    static purchase(phone, productId, callback) {
        //Getting token
        redis.hget('platform:tokens', 'beeline-payments', function (err, token) {
            if (err) {
                log.error('Failed to get Beeline payment auth token:', err);
                return callback(true);
            } else {
                let time = moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DDTHH:mmZ');
                axios({
                    method: 'POST',
                    url: settings.beeline.payments + "/game/list",
                    headers: {
                        'Authorization' : 'Bearer '+ token,
                        'Content-type' : 'application/json'
                    },
                    data: {
                        "phone" : phone.toString(),
                        "productId": parseInt(productId),
                        "time": time,
                        "signature": sha.encrypt(phone.toString()+productId+time+settings.beeline.partner)
                    },
                    timeout: 30000
                }).then(response => {
                    log.debug('Purchase completed:', response.data);

                    callback();
                }).catch(err => {
                    log.error('Failed to process Beeline purchase:', err);
                    callback(true);
                });
            }
        })
    }
}

module.exports = Payments;