let log = require('../services/bunyan').log;
let _ = require('lodash');
let jws = require('../services/jws');
const send = require('@polka/send-type');
const bulk = require('../services/bulk');
const moment = require("moment");
const timeZone = require("moment-timezone");

class Tokens {

    static Decrypt(req, res, next) {

        //og.info('Verifying query tokens', req.body.token);
        if (req.body.token === null || req.body.token === undefined) {
            log.error('Token was not sent, so it cannot be verified (500):', req.body.token, req.body);

            return send(res, 500, { status: 'token_not_send' });
        }

        if (jws.verify(req.body.token)) {

            let token = jws.decrypt(req.body.token);
            if (!token) return send(res, 500, { status: 'failed' });

            if (token.game_id !== req.body.game.game_id) {
                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : token.profile_id,
                    player_id: token.player_id,
                    event : 'accelera-api',
                    page : 'token',
                    status: 'unvalid-game-token',
                    game_id : (req.body.game.game_id === undefined) ? "" : req.body.game.game_id,
                    context : req.body.token,
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };
                log.error('Unvalid game token', req.path, token, token.profile_id, token.player_id, token.game_id, req.body.game.game_id );
                bulk.store(req.body.game.game_id, JSON.stringify(data), function () {});

                return send(res, 500, { status: 'unvalid game token' });
            } else {
                req.body.profile_id = token.profile_id;
                req.body.player_id = token.player_id;
                req.body.decrypted_token = _.cloneDeep(token);
                req.body.activated = token.activated;
                next();
                
                //Token lifetime check
                // if (token.token_created === undefined) send(res, 500, { status: "token_not_active" });

                // if (token.token_created !== undefined) {
                //     if (token.token_created + 1800000 < Math.floor(new Date())) {
                //     log.error(
                //         "Token lifetime is not valid:",
                //         token.profile_id,
                //         token.player_id,
                //     );
                //     return send(res, 500, { status: "token_not_active" });

                //     } else {
                //     }
                // }
            }

        } else {
            log.error('Token not verified:', req.body.token);

            try {
                let token = jws.decrypt(req.body.token);
                req.body.profile_id = token.profile_id;
                req.body.player_id = token.player_id;

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    player_id: req.body.player_id,
                    event : 'accelera-api',
                    page : 'token',
                    status: 'failed-to-verify',
                    game_id : (req.body.game.game_id === undefined) ? "" : req.body.game.game_id,
                    context : req.body.token,
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                bulk.store(req.body.game.game_id, JSON.stringify(data), function () {});

            } catch (e){
                log.error('Token verification status cannot be stored to clickhouse:', req.body.token, e);
            }

            return send(res, 500, { status: 'failed' });
        }
    }

    static TrustedDecrypt(req, res, next) {

        let token = jws.decrypt(req.query.token);

        if (!token) return send(res, 500, { status: 'failed' });

        req.body.game_id = token.game_id;
        req.body.fingerprint = token.fingerprint;
        req.body.profile_id = token.profile_id;
        req.body.player_id = token.player_id;
        next();
    }
}

module.exports = Tokens;