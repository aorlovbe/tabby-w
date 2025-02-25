let log = require('../services/bunyan').log;
let redis = require('../services/redis').redisclient;
let _ = require('lodash');
let jws = require('../services/jws');
const send = require('@polka/send-type');
const axios = require("axios");
const settings = require("../settings");
const moment = require("moment");
const momentTimezone = require("moment-timezone");
const bulk = require("../services/bulk");

class Multiplayer {

    static getAvailableNodes(req, res, next) {

        redis.hgetall('platform:nakama', function (err, nodes){
            if (err) return send(res, 500, { status: 'failed' });

            if (req.body.game.private.multiplayer.available === true) {
                let _nodes = (_.size(nodes) === 0) ? {} : isJSONall(nodes);
                let healthy_nodes = [];

                for (let i in _nodes) {
                    if (_nodes[i].health !== -1 && (req.body.game.private.multiplayer.nodes.includes(_nodes[i].node) === true)) {
                        _nodes[i].node =  _nodes[i].node.split('://')[1];
                        healthy_nodes.push(_nodes[i]);
                    }
                }

                req.body.nodes = _.sortBy([healthy_nodes], ['session_count', 'match_count'])[0];
                next();

            } else {
                return send(res, 404, { status: 'closed', nodes: [] });
            }
        })
    }

    static updateAvatarOnNodes(req, res, next) {

        log.debug('Updating avatar on game nodes:', req.body.game.private.multiplayer.nodes);

        redis.hgetall('platform:nakama', function (err, nodes){
            if (err) return send(res, 500, { status: 'failed' });

            log.debug('Nakama total nodes are:', nodes);

            let _nodes = (_.size(nodes) === 0) ? {} : isJSONall(nodes);

            for (let i in _nodes) {
                //Check if node is for current game & update avatar on node
                if (req.body.game.private.multiplayer.nodes.includes(_nodes[i].node) === true) {
                    log.debug('Avatar will be updated (http key request only):', _nodes[i].node, req.body.avatar, req.body.profile_id);
                    axios({
                        method: 'POST',
                        url: _nodes[i].node + ":7350/v2/rpc/updateAvatar",
                        params: {
                            "http_key" : req.body.game.private.multiplayer.http_key,
                            "unwrap" : "unwrap"
                        },
                        headers: {
                            'Content-type' : 'application/json',
                            'Accept' : 'application/json'
                        },
                        data: JSON.stringify({
                                "profile_id" : req.body.profile_id,
                                "avatar": req.body.avatar
                            }),
                        timeout: 30000
                    }).then(response => {
                        log.debug('Avatar was updated:', _nodes[i].node, req.body.avatar);

                        //Update analytics
                        let event = {
                            "event"  : "accelera-api",
                            "page" : 'avatar',
                            "status" : "updated",
                            "game_id": req.body.game.game_id,
                            "details" : req.body.avatar.toString(),
                            "profile_id" : req.body.profile_id,
                            "player_id" : (req.body.player_id === undefined) ? '' : req.body.player_id.toString(),
                            "timestamp" : Math.floor(new Date()),
                            "date" : moment(new Date()).format('YYYY-MM-DD'),
                            "time": moment(new Date()).format('HH:mm'),
                            "datetime": moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                        };

                        bulk.store(req.body.game.game_id, JSON.stringify(event), function (err) {
                            if (err) {
                                log.error('Error while storing webhooks messages for Clickhouse bulk:', err);
                            }
                        });

                    }).catch(err => {
                        log.error('Failed to update avatar:', _nodes[i].node, req.body.avatar, err);
                    });
                } else {
                    log.debug('No nodes to update')
                }
            }

            next();

        })
    }

    static banProfileOnNodes(req, res, next) {

        log.debug('Banning on game nodes:', req.body.game.private.multiplayer.nodes);

        redis.hgetall('platform:nakama', function (err, nodes){
            if (err) return log.error('Failed to ban:', req.body.profile_id);

            log.debug('Nakama total nodes are:', nodes);

            let _nodes = (_.size(nodes) === 0) ? {} : isJSONall(nodes);

            for (let i in _nodes) {
                //Check if node is for current game & update avatar on node
                if (req.body.game.private.multiplayer.nodes.includes(_nodes[i].node) === true) {
                    log.debug('Profile will be banned (http key request only):', _nodes[i].node, req.body.profile_id);
                    axios({
                        method: 'POST',
                        url: _nodes[i].node + ":7350/v2/rpc/banProfile",
                        params: {
                            "http_key" : req.body.game.private.multiplayer.http_key,
                            "unwrap" : "unwrap"
                        },
                        headers: {
                            'Content-type' : 'application/json',
                            'Accept' : 'application/json'
                        },
                        data: JSON.stringify({
                            "profile_id" : req.body.profile_id
                        }),
                        timeout: 30000
                    }).then(response => {
                        log.debug('Profile was banned:', _nodes[i].node, req.body.profile_id);

                    }).catch(err => {
                        log.error('Failed to ban:', _nodes[i].node, req.body.profile_id, err);
                    });
                } else {
                    log.debug('No nodes to update')
                }
            }

            next();

        })
    }

    static unbanProfileOnNodes(req, res, next) {

        log.debug('Banning on game nodes:', req.body.game.private.multiplayer.nodes);

        redis.hgetall('platform:nakama', function (err, nodes){
            if (err) return log.error('Failed to unban:', req.body.profile_id);

            log.debug('Nakama total nodes are:', nodes);

            let _nodes = (_.size(nodes) === 0) ? {} : isJSONall(nodes);

            for (let i in _nodes) {
                //Check if node is for current game & update avatar on node
                if (req.body.game.private.multiplayer.nodes.includes(_nodes[i].node) === true) {
                    log.debug('Profile will be unbanned (http key request only):', _nodes[i].node, req.body.profile_id);
                    axios({
                        method: 'POST',
                        url: _nodes[i].node + ":7350/v2/rpc/unbanProfile",
                        params: {
                            "http_key" : req.body.game.private.multiplayer.http_key,
                            "unwrap" : "unwrap"
                        },
                        headers: {
                            'Content-type' : 'application/json',
                            'Accept' : 'application/json'
                        },
                        data: JSON.stringify({
                            "profile_id" : req.body.profile_id
                        }),
                        timeout: 30000
                    }).then(response => {
                        log.debug('Profile was unbanned:', _nodes[i].node, req.body.profile_id);
                    }).catch(err => {
                        log.error('Failed to unban:', _nodes[i].node, req.body.profile_id, err);
                    });
                } else {
                    log.debug('No nodes to update')
                }
            }

            next();

        })
    }
}

function isJSONstring(value) {
    try {
        JSON.parse(value);
    } catch (e) {
        return value;
    }
    return JSON.parse(value);
}

function isJSON(json) {
    let result = (_.isObject(json) === true) ? JSON.stringify(json) : json;
    return result;
}

function isJSONall(objects) {

    let object = {};
    if (_.size(objects) !== 0) {
        let i = 0;
        _.forEach(objects, function (value, key) {
            object[key] = isJSONstring(value);
            i++;
        });

        if (i === _.size(objects)) {
            return object;
        }
    } else {
        return object;
    }
}


module.exports = Multiplayer;