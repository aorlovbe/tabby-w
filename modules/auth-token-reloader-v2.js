const axios = require("axios");
const settings = require("../settings");
const sha = require("../services/sha");
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const log = require("../services/bunyan").log;
const redis = require('../services/redis').redisclient;
process.env['NODE_TLS_REJECT_UNAUTHORIZED'] = '0';

log.debug('Token manager (api/v2) is started');

setInterval(() => {
    reload();
}, 60000*10); //each 10 min

function reload(){
    let headers = {
        'Content-Type' : 'application/json',
        'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
    };

    let time = moment(momentTimezone.tz('Europe/Moscow')._d).subtract(3, 'hour').format('YYYY-MM-DDTHH:mm:ssZ');
    let phone = 79880001893;
    let appID = 'cubesolutions@localhost.ru';
    let secret = 'testkey';
    let url = 'https://old.partnerka.beeline.ru/api';
    //sha1('testapp999999999912022-03-28T17:16:24+03:00secretKey').

    let checksum = sha.encrypt(appID + phone.toString()+time+secret);

    axios({
        method: 'POST',
        url: url + "/v2/game/token",
        headers: headers,
        data: {
            "phone" : phone,
            "appID" : appID,
            "time" : time,
            "signature" : checksum
        },
        timeout: 30000
    }).then(response => {
        log.info('[info] Authorized (api/v2) token:', response.data);

    }).catch(err => {
        log.error('Failed to get Beeline payment auth token:', err, url);
    });
}

reload();