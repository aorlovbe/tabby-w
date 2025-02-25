let log = require('../services/bunyan').log;
let redis = require('../services/redis').redisclient;
let _ = require('lodash');
let jws = require('../services/jws');
let bulk = require('../services/bulk');
const send = require('@polka/send-type');
const axios = require("axios");
const settings = require("../settings");
const Leaderboard = require('../api/leaderboard');
const Profiles = require('../api/profiles');
const Games = require('../api/games');
const Achievements = require('../api/achievements');
const Counters = require('../api/counters');
const API = require('../middleware/api');
const Multiplayer = require('../middleware/multiplayer');
const Probability = require('../services/probabilities');
const moment = require("moment");
const timeZone = require("moment-timezone");
const Rewards = require("../api/rewards");

class Nakama {

    static processMatchData(req, res, next) {
        log.debug('Processing match data:', req.body.game_id, req.body.match);
        let match = req.body;
        switch (req.body.status) {
            case ("completed") : {
                for (let i in req.body.presences) {
                    Profiles.get(req.body.presences[i].username, function (err, profile) {
                        if (profile !== {}) {

                            //Check if profile is blocked
                            Profiles.is_block(profile.profile_id, function (blocked){
                                Profiles.is_ban(profile.profile_id, function (banned){
                                    if (!blocked && !banned) {
                                        //Storing point to user ID
                                        Leaderboard.modify({"body" : {
                                                "system" : req.body.game_id,
                                                "name" : "points",
                                                "value" : req.body.presences[i].rewards.points,
                                                "profile_id" : profile.id
                                            }}, function (){})

                                        //Storing collection to achievements
                                        Achievements.create({"body" : {
                                                "game_id" : req.body.game_id,
                                                "name" : req.body.presences[i].rewards.collection.toString(),
                                                "profile_id" : profile.profile_id
                                            }}, function (){

                                            //Getting achievements (collection) to define size for event
                                            Achievements.findbyprofile({body: {profile_id: profile.profile_id}}, function (err, collection){
                                                if (err) {
                                                    log.error('Cannot get connection after created new element:',profile.profile_id, err);
                                                } else {
                                                    //Player event
                                                    let match = req.body
                                                    let player_context = {
                                                        timestamp : Math.floor(new Date()),
                                                        status: "completed",
                                                        match: match.match,
                                                        round: match.round.round,
                                                        position: match.presences[i].rewards.position,
                                                        rewards: match.presences[i].rewards,
                                                        achievements: Object.keys(collection),
                                                        achievements_size: Object.keys(collection).length,
                                                        result: match.presences[i].round_result,
                                                        figure: match.presences[i].figure,
                                                        node: match.presences[i].node,
                                                        profile_id: profile.profile_id,
                                                        player_id: profile.id,
                                                        avatar: match.presences[i].avatar,
                                                        session: match.presences[i].sessionId,
                                                        userId: match.presences[i].userId,
                                                        game_id: match.game_id,
                                                        date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                                                        time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                                                        datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                                    }

                                                    API.publish(profile.profile_id, 'game', player_context, function (){})

                                                    //Updating auth table
                                                    bulk.store('matches', JSON.stringify({
                                                        timestamp : Math.floor(new Date()),
                                                        status: "completed",
                                                        match: match.match,
                                                        round: match.round.round,
                                                        standoff: match.round.standoff,
                                                        position: match.presences[i].rewards.position,
                                                        rewards: JSON.stringify(match.presences[i].rewards),
                                                        achievements: Object.keys(collection),
                                                        achievements_size: Object.keys(collection).length,
                                                        result: match.presences[i].round_result,
                                                        figure: match.presences[i].figure,
                                                        node: match.presences[i].node,
                                                        profile_id: profile.profile_id,
                                                        player_id: profile.id,
                                                        avatar: match.presences[i].avatar,
                                                        session: match.presences[i].sessionId,
                                                        userId: match.presences[i].userId,
                                                        game_id: match.game_id,
                                                        date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                                                        time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                                                        datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                                    }), function () {});
                                                }
                                            })
                                        })

                                    } else {
                                        log.warn('[!] Profile is blocked / banned, leaderboard will not be updated:', profile.id, req.body.presences[i])
                                    }
                                })

                            })
                        }
                    })
                }

                let names = [];
                let sessions = [];
                let avatars = [];
                for (let i in match.presences) {
                    names.push(match.presences[i].username);
                    sessions.push(match.presences[i].sessionId);
                    avatars.push(match.presences[i].avatar);
                };

                let context = {
                    match: match.match,
                    round: match.round.round,
                    users: Object.keys(match.presences),
                    profiles: names,
                    sessions: sessions,
                    avatars: avatars,
                    players_count: Object.keys(match.presences).length,
                    game_id: match.game_id
                }

                //Match event
                API.publish(match.match, 'completed', context, function (){})

                break;
            }

            case "started" : {
                let match = req.body;
                let names = [];
                let sessions = [];
                let avatars = [];
                let users = [];
                for (let i in match.presences) {
                    names.push(match.presences[i].username);
                    sessions.push(match.presences[i].sessionId);
                    avatars.push(match.presences[i].avatar);
                    users.push(match.presences[i].userId);
                };

                let context = {
                    match: match.match,
                    users: Object.keys(match.presences),
                    profiles: names,
                    sessions: sessions,
                    avatars: avatars,
                    players_count: Object.keys(match.presences).length,
                    game_id: match.game_id
                }

                API.publish(match.match, 'started', context, function (){})

                //Updating match table
                let stat = (match.round.round === 1 && match.round.standoff === 0) ? "created" : "started";
                stat = (match.round.standoff !== 0) ? "standoff" : "started";

                bulk.store('matches', JSON.stringify({
                    timestamp : Math.floor(new Date()),
                    status: stat,
                    match: match.match,
                    round: match.round.round,
                    standoff: match.round.standoff,
                    node: match.match.split('.')[1],
                    profile_id: JSON.stringify(names),
                    avatar: JSON.stringify(avatars),
                    session: JSON.stringify(sessions),
                    userId: JSON.stringify(users),
                    game_id: match.game_id,
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                }), function () {});

                log.info('Match',stat,':', JSON.stringify(req.body))

                break;
            }

            case "validation" : {
                let match = req.body;
                let names = [];
                let sessions = [];
                let avatars = [];
                let users = [];
                let figures = [];
                let round_results = [];

                for (let i in match.presences) {
                    names.push(match.presences[i].username);
                    sessions.push(match.presences[i].sessionId);
                    avatars.push(match.presences[i].avatar);
                    users.push(match.presences[i].userId);
                    figures.push(match.presences[i].figure);
                    round_results.push(match.presences[i].round_result);
                };

                let context = {
                    match: match.match,
                    users: Object.keys(match.presences),
                    profiles: names,
                    sessions: sessions,
                    avatars: avatars,
                    figures: figures,
                    round_results: round_results,
                    players_count: Object.keys(match.presences).length,
                    game_id: match.game_id
                }

                API.publish(match.match, 'validation', context, function (){})

                //Updating match table
                bulk.store('matches', JSON.stringify({
                    timestamp : Math.floor(new Date()),
                    status: 'validation',
                    match: match.match,
                    round: match.round.round,
                    standoff: match.round.standoff,
                    node: match.match.split('.')[1],
                    profile_id: JSON.stringify(names),
                    avatar: JSON.stringify(avatars),
                    session: JSON.stringify(sessions),
                    userId: JSON.stringify(users),
                    result: JSON.stringify(round_results),
                    figure: JSON.stringify(figures),
                    game_id: match.game_id,
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                }), function () {});

                log.info('Match validation:', JSON.stringify(req.body));

                break;
            }

            case "closed" : {
                let match = req.body;

                let context = {
                    match: match.match,
                    game_id: match.game_id
                }

                log.warn('Match closed:', JSON.stringify(req.body))

                API.publish(match.match, 'closed', context, function (){})

                break;
            }

            default : {
                log.info('Match default:', JSON.stringify(req.body))

                break;
            }
        }
        next();
    }

    static getMatchRewards(req, res, next) {
        log.debug('Processing match rewards:', req.body.game_id, req.body.match, '/ players are:', Object.keys(req.body.presences).length);

        Probability.getItemByProbability(req, function (err, rewards, prob) {
            if (err) return send(res, 200, {"status" : 'failed', "rewards" : {}});

            //Decreasing tries counter for each player
            for (let i in req.body.presences) {
                let player = {
                    "profile_id" : req.body.presences[i].username,
                    "game_id" : req.body.game_id,
                    "name" : "tries",
                    "value" : -1
                }
                Counters.modify({body : player}, function (err, updates){
                    if (err || updates.tries < 0) {
                        log.error('Wow, player wants to play without balance. Will be kicked in Accelera', req.body.match, JSON.stringify(req.body.presences), req.body.presences[i].username, req.body.game_id, JSON.stringify(updates), err)

                        //Block profile
                        Profiles.block(req.body.presences[i].username, function (err, ok){})

                        API.publish(req.body.presences[i].username, 'ban', {
                            "profile_id" : req.body.presences[i].username,
                            "game_id" : req.body.game_id,
                            "match" : req.body.match,
                            "sessionId" : req.body.presences[i].sessionId,
                            "avatar": req.body.presences[i].avatar,
                            "reason": "Попытка игры с отрицательным балансом попыток (после списания): " +  updates.tries + ' / ' + req.body.presences[i].username
                        }, function (){})
                    }
                })
            }

            req.body.rewards = rewards;

            //Prepare event for Accelera
            let match = req.body;
            let names = [];
            let sessions = [];
            let avatars = [];

            for (let i in match.presences) {
                names.push(match.presences[i].username);
                sessions.push(match.presences[i].sessionId);
                avatars.push(match.presences[i].avatar);
            };

            let context = {
                match: match.match,
                node: match.match.split('.')[1],
                users: Object.keys(match.presences),
                profiles: names,
                sessions: sessions,
                avatars: avatars,
                players_count: Object.keys(match.presences).length,
                game_id: match.game_id,
                points: match.rewards.points,
                collection: match.rewards.collection,
                partners: match.rewards.partners
            }

            send(res, 200, {"status" : 'ok', "rewards" : req.body.rewards});

            API.publish(match.match, 'match', context, function (){})

            //Antifraud counts for match players
            let sortednames = names.sort();
            redis.multi()
                .zincrby('platform:performed-matches:pairs', 1, sortednames[0]+'-'+sortednames[1])
                .zincrby('platform:performed-matches:pairs', 1, sortednames[0]+'-'+sortednames[2])
                .zincrby('platform:performed-matches:pairs', 1, sortednames[0]+'-'+sortednames[3])
                .zincrby('platform:performed-matches:pairs', 1, sortednames[1]+'-'+sortednames[2])
                .zincrby('platform:performed-matches:pairs', 1, sortednames[1]+'-'+sortednames[3])
                .zincrby('platform:performed-matches:pairs', 1, sortednames[2]+'-'+sortednames[3])
                .exec(function (){})

            next();
        })
    }

    static setMatchJoins(req, res, next) {
        log.info('Got join to match info, storing for further antifraud:', req.body.match, req.body.profile_id);
        redis.multi()
            .sadd('platform:pending-matches:'+req.body.match, req.body.profile_id)
            .expire('platform:pending-matches:'+req.body.match, 120) //2 min
            .exec(function (){
                next();
            })
    }

    static AntifraudCheckup(req, res, next) {
        log.info('Investigating player for antifraud in planned match:', req.body.match, req.body.profile_id);
        redis.smembers('platform:pending-matches:'+req.body.match, function (err, pres){
            let players = pres.sort();

            if (players.length !== 0) {
                //There is a waiting players
                switch (players.length) {
                    case (1) : {
                        players.push(req.body.profile_id);
                        let sorted = players.sort();

                        redis.zscore('platform:performed-matches:pairs',sorted[0] + '-' + sorted[1], function (err, score){
                            log.info('Investigatins scores:',score,sorted);
                            if (score !== null) {
                                if (score >= 6) {
                                    log.info('[error] Investigation decision is:', 'false',req.body.profile_id);
                                    req.body.decision = 'false';
                                    next();
                                } else {
                                    log.info('Investigation decision is:', 'true',req.body.profile_id);
                                    req.body.decision = 'true';
                                    next();
                                }
                            } else {
                                //We didn't play yet
                                log.info('Investigation decision is:', 'true',req.body.profile_id);
                                req.body.decision = 'true';
                                next();
                            }
                        })

                        break;
                    }

                    case (2) : {
                        players.push(req.body.profile_id);
                        let sorted = players.sort();

                        redis.multi()
                            .zscore('platform:performed-matches:pairs',sorted[0] + '-' + sorted[1])
                            .zscore('platform:performed-matches:pairs',sorted[0] + '-' + sorted[2])
                            .zscore('platform:performed-matches:pairs',sorted[1] + '-' + sorted[2])
                            .exec(function (err, scores) {
                                //Excluding nulls
                                log.info('Investigatins scores:',scores,sorted);
                                let results = scores.filter(score => score !== null);
                                for (let i in results) {
                                    if (results[i] >= 6) {
                                        log.info('[error] Investigation decision is:', 'false',req.body.profile_id);
                                        req.body.decision = 'false';
                                        return next();
                                    }
                                }
                                log.info('Investigation decision is:', 'true',req.body.profile_id);
                                req.body.decision = 'true';
                                next();
                            })
                        break;
                    }

                    case (3) : {
                        players.push(req.body.profile_id);
                        let sorted = players.sort();

                        redis.multi()
                            .zscore('platform:performed-matches:pairs',sorted[0] + '-' + sorted[1])
                            .zscore('platform:performed-matches:pairs',sorted[0] + '-' + sorted[2])
                            .zscore('platform:performed-matches:pairs',sorted[0] + '-' + sorted[3])
                            .zscore('platform:performed-matches:pairs',sorted[1] + '-' + sorted[2])
                            .zscore('platform:performed-matches:pairs',sorted[1] + '-' + sorted[3])
                            .zscore('platform:performed-matches:pairs',sorted[2] + '-' + sorted[3])
                            .exec(function (err, scores,) {
                                //Excluding nulls
                                log.info('Investigatins scores:',scores,sorted);
                                let results = scores.filter(score => score !== null);
                                for (let i in results) {
                                    if (results[i] >= 6) {
                                        log.info('[error] Investigation decision is:', 'false',req.body.profile_id);
                                        req.body.decision = 'false';
                                        return next();
                                    }
                                }
                                log.info('Investigation decision is:', 'true',req.body.profile_id);
                                req.body.decision = 'true';
                                next();
                            })
                        break;
                    }
                }

            } else {
                log.info('Investigation decision is:', 'true',req.body.profile_id);
                req.body.decision = 'true';
                next();
            }
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


module.exports = Nakama;