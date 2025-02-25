let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const accounts              = 'platform:users';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');

class Users {
    static save(user, callback) {
        this.find(user, function (err, account) {
            if (err) return callback(true, err);
            if (account === null) {
                log.info('Creating new account:', user);
                let account = {
                    timestamp : Math.floor(new Date()),
                    id : user.id.toString(),
                    username : (user.username === undefined) ? "" : user.username,
                    email : (user.email === undefined) ? "" : user.email,
                    name : (user.name === undefined) ? "" : user.name,
                    surname : (user.surname === undefined) ? "" : user.surname,
                    gender : (user.gender === undefined) ? "" : user.gender,
                    social : (user.social === undefined) ? "" : user.social,
                    fingerprint: (user.fingerprint === undefined) ? "" : JSON.stringify(user.fingerprint),
                    status : "registered",
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                //Updating auth table
                Bulk.store('auth', account, function () {});

                //Registration
                redis.hset(accounts, md5.md5(user.id), JSON.stringify(account), (err) => {
                    if(err) {
                        log.error('Can\'t store user:' + err.message);
                        callback(true);
                    } else {
                        log.info('New user created:', JSON.stringify(account));
                        callback(null, true);
                    }
                });

            } else {

                Bulk.store('auth', {
                    timestamp : Math.floor(new Date()),
                    id : account.id,
                    username : account.username,
                    email : account.email,
                    name : account.name,
                    surname : account.surname,
                    gender : account.gender,
                    social : account.social,
                    status : 'returned',
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                },function () {});

                redis.hset(accounts, md5.md5(account.id), JSON.stringify(account), (err) => {
                    if(err) {
                        log.error('Can\'t store user:' + err.message);
                    } else {
                        log.info('User updated:', JSON.stringify(account));
                        callback(null, false);
                    }
                });
            }
        })
    }

    static register(user, callback) {
        this.find(user, function (err, account) {
            if (err) return callback(true);

            if (account === null) {
                log.info('Registering new account:', user);
                let account = {
                    timestamp : Math.floor(new Date()),
                    id : user.id.toString(),
                    username : (user.username === undefined) ? "" : user.username,
                    email : (user.email === undefined) ? "" : user.email,
                    name : (user.name === undefined) ? "" : user.name,
                    surname : (user.surname === undefined) ? "" : user.surname,
                    gender : (user.gender === undefined) ? "" : user.gender,
                    social : (user.social === undefined) ? "" : user.social,
                    status : "registered",
                    fingerprint: (user.fingerprint === undefined) ? "" : JSON.stringify(user.fingerprint),
                    game_id: user.game_id,
                    date : moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD'),
                    time :  moment(timeZone.tz('Europe/Moscow')).format('HH:mm'),
                    datetime : moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                //Updating auth table
                Bulk.store('auth', account, function () {});

                //Registration
                redis.hset(accounts, md5.md5(user.id), JSON.stringify(account), (err) => {
                    if(err) {
                        log.error('Can\'t store user:' + err.message);
                        callback(true);
                    } else {
                        log.debug('New user created:', JSON.stringify(account));
                        callback(false, true, account);
                    }
                });

            } else {
                log.debug('User already registered:', JSON.stringify(account));
                callback(false, false, account);
            }
        })
    }

    static find(details, callback) {
        log.info('Searching user by social ID:', details.id);
        redis.hget(accounts, md5.md5(details.id), function (err, result) {
            if (err || result == null) {
                log.info(`User ${details.id} not found:`, md5.md5(details.id));
                return callback(null, null);
            } else {
                log.info('User is found by account:', details.id);
                return callback(null, JSON.parse(result));
            }
        });
    }

}

module.exports = Users;