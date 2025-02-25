let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const tasks              = 'platform:profile:';
const profiles              = 'platform:games:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const Task = require('./tasks');
const nanoid = require('../services/nanoid');
const momentTimezone = require("moment-timezone");

class Tasks {
    static find(req, callback) {
        log.info('Searching tasks by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, {});
            } else {
                redis.hgetall(tasks + profile + ":tasks", function (err, result) {
                    if (err || result === null) {
                        log.info('There are no tasks found for:', req.user.id, profile);
                        return callback(false, {});
                    } else {
                        log.info('Tasks found:', _.size(result));
                        if (_.size(result) !== 0) {
                            let i = 0;
                            let data = {};
                            _.forEach(result, function (value, key) {
                                _.set(data, key, isJSONstring(value));
                                i++;
                            })

                            if (i === _.size(result)) {
                                //Grouped tasks by status (active / completed)
                                return callback(null, data);
                            }
                        }
                    }
                });
            }

        });
    }

    static findbyprofile(req, callback) {
        log.info('Searching tasks by profile ID:', req.body.profile_id);
        redis.hgetall(tasks + req.body.profile_id + ":tasks", function (err, result) {
            if (err || result === null) {
                log.info('There are no tasks found for:', req.body.profile_id);
                return callback(false, {});
            } else {
                log.info('Tasks found:', _.size(result));
                if (_.size(result) !== 0) {
                    let i = 0;
                    let data = {};
                    _.forEach(result, function (value, key) {
                        _.set(data, key, isJSONstring(value));
                        i++;
                    })

                    if (i === _.size(result)) {
                        //Grouped tasks by status (active / completed)
                        return callback(null, _.groupBy(data, "status"));
                    }
                }
            }
        });
    }

    static create(req, callback) {
        log.info('Searching tasks by profile ID:', req.body.profile_id);

        let id = (req.body.unique === 'true') ? nanoid.get() : req.body.name;
        req.body.timestamp = Math.floor(new Date());
        req.body.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
        req.body.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
        req.body.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');


        redis.hset(tasks + req.body.profile_id + ":tasks", id, isJSON(req.body), function (err, result) {
            if (err) {
                log.error('Task cannot be stored:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Task is created:', result, tasks + req.body.profile_id + ":tasks");

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'created',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    context : isJSON(req.body),
                    name : id.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('tasks', data, function () {});

                // Storing tasks to a list for FTP
                // num will be generated by worker
                // num;profile_id;game_id;task_id;task;datetime;status
                let profile_id = req.body.profile_id;
                let game_id = (req.body.game_id === 'rock-paper-scissors') ? 3 : 0;
                let task_id = id.split('-')[1]
                let task = id;
                let datetime = moment(momentTimezone.tz('Europe/Moscow')).format('YYYYMMDD_HHmmss');
                let status = 'created';

                let input = [profile_id,game_id,task_id,task,datetime,status].join(';');

                log.info('Storing new task in the list by profile ID:', req.body.profile_id);
                //Storing new mission to the list. List will be drained automatically by schedule
                redis.RPUSH(profiles + req.body.game_id + ":tasks:list", input, function (err, result) {
                    if (err) {
                        log.error('Cannot store row into the Redis missions batch:', input);
                        callback(true);
                    } else {
                        log.info('Task is stored to the list:', req.body.name, result, req.body.profile_id);

                        let data = {
                            timestamp : Math.floor(new Date()),
                            profile_id : req.body.profile_id,
                            status : 'in-the-list',
                            game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                            context : input,
                            name : id.toString(),
                            date : moment(new Date()).format('YYYY-MM-DD'),
                            time: moment(new Date()).format('HH:mm'),
                            datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                        };

                        Bulk.store('tasks', data, function () {});

                        callback(false, id);
                    }
                })

            }
        });
    }


    static storetolist(req, input, callback) {
        log.info('Storing new task in the list by profile ID:', req.body.profile_id);
        //Storing new mission to the list. List will be drained automatically by schedule
        redis.RPUSH(profiles + req.body.game_id + ":tasks:list", input, function (err, result) {
            if (err) {
                log.error('Cannot store row into the Redis missions batch:', input);
                callback(true);
            } else {
                log.info('Task is stored to the list:', result, tasks + req.body.profile_id + ":tasks:list");

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'in-the-list',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    context : input,
                    name : req.body.name.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('tasks', data, function () {});

                callback();
            }
        })
    }

    static createmultiple(req, callback) {
        log.info('Creating tasks by names:', req.body);

        let i = 0;

        _.forEach(req.body, function (value) {

            redis.hset(tasks + value.profile_id + ":tasks", value.name, isJSON(value), function (err, result) {
                if (err) {
                    log.error('Task cannot be stored:', value);
                } else {
                    log.info('Task is created:', result);

                    let data = {
                        timestamp : Math.floor(new Date()),
                        profile_id : value.profile_id,
                        status : 'created',
                        game_id : (value.game_id === undefined) ? "" : value.game_id,
                        context : isJSON(value),
                        name : value.name.toString(),
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store('tasks', data, function () {});

                }

                i++;

                if (i === _.size(req.body)) {
                    return callback();
                }
            });

        });
    }

    static remove(req, callback) {
        log.info('Searching tasks by profile ID:', req.body.profile_id);
        redis.hdel(tasks + req.body.profile_id + ":tasks", req.body.name, function (err, result) {
            if (err) {
                log.error('Task cannot be deleted:', req.body.profile_id, req.body);
                return callback(true);
            } else {
                log.info('Task is deleted:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'removed',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    name : req.body.name.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('tasks', data, function () {});
                return callback()
            }
        });
    }

    static modify(req, callback) {
        log.info('Searching tasks by profile ID:', req.body.profile_id);
        redis.hget(tasks + req.body.profile_id + ":tasks", req.body.name, function (err, result) {
            if (err) {
                log.error('Task cannot be modified:', req.body.profile_id, req.body);
                return callback(true);
            } else {

                if (result !== null) {
                    let task = JSON.parse(result);

                    if (_.size(req.body) !== 0) {
                        let i = 0;
                        _.forEach(req.body, function (value, key) {
                            _.set(task, key, value);
                            i++;
                        });

                        if (i === _.size(req.body)) {
                            task.timestamp = Math.floor(new Date());
                            task.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
                            task.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
                            task.datetime = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD HH:mm:ss');

                            redis.hset(tasks + req.body.profile_id + ":tasks", req.body.name, JSON.stringify(task), function() {
                                log.info('Task is updated:', JSON.stringify(task));

                                let data = {
                                    timestamp : Math.floor(new Date()),
                                    profile_id : req.body.profile_id,
                                    status : 'modified',
                                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                    context : JSON.stringify(task),
                                    name : req.body.name.toString(),
                                    date : moment(new Date()).format('YYYY-MM-DD'),
                                    time: moment(new Date()).format('HH:mm'),
                                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                };

                                Bulk.store('tasks', data, function () {});

                                callback();
                            });
                        }
                    } else {
                        log.info('Nothing to update');
                        callback();
                    }
                } else {
                    log.info('Nothing to update, no tasks');
                    callback();
                }
            }
        });
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

module.exports = Tasks;