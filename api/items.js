let log                     = require('../services/bunyan').log;
const redis                 = require('../services/redis').redisclient;
const aes                   = require('../services/aes');
const md5                   = require('../services/md5');
let _                       = require('lodash');
const personal_items        = 'platform:profile:';
const items              = 'platform:games:';
const profiles              = 'platform:games:';
const moment = require('moment');
const timeZone = require('moment-timezone');
const Bulk = require('./bulk');
const nanoid = require('../services/nanoid');
const momentTimezone = require("moment-timezone");

class Items {
    static find(req, callback) {
        log.info('Searching items by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, {});
            } else {
                redis.multi()
                    .hgetall(items + req.body.game_id + ":items")
                    .hgetall(personal_items + profile + ":items")
                    .hgetall(personal_items + profile + ":locks")
                    .hgetall(personal_items + profile + ":purchases")
                    .exec(function (err, results) {
                    if (err) {
                        log.error('Error while getting items for the game:', req.user.id, profile);
                        return callback(true, {"personal" : {}, "basic" : {}, "purchases" : {}, "locks" : {}});
                    } else {
                        log.info('Items found:', _.size(results[0]), _.size(results[1]), _.size(results[2]));
                        let basic = (_.size(results[0]) === 0) ? {} : isJSONall(results[0]);
                        let personal = (_.size(results[1]) === 0) ? {} : isJSONall(results[1]);
                        let locks = (_.size(results[2]) === 0) ? {} : isJSONall(results[2]);
                        let purchased = (_.size(results[3]) === 0) ? {} : isJSONall(results[3]);

                        return callback(null, {"personal" : personal, "basic" : basic, "purchases" : purchased, "locks" : locks});
                    }
                });
            }

        });
    }

    static findbyprofile(req, callback) {
        //log.info('Searching items by profile ID:', req.body.profile_id);
        redis.multi()
            .hgetall(items + req.body.game_id + ":items")
            .hgetall(personal_items + req.body.profile_id + ":items")
            .hgetall(personal_items + req.body.profile_id + ":locks")
            .hgetall(personal_items + req.body.profile_id + ":purchases")
            .exec(function (err, results) {
                if (err) {
                    log.error('Error while getting items for the game:', req.body.profile_id);
                    return callback(true, {"personal" : [], "basic" : [], "purchases" : [], "locks" : {}});
                } else {
                    //log.info('Items found:', _.size(results[0]), _.size(results[1]), _.size(results[2]));
                    let basic = (_.size(results[0]) === 0) ? [] : _.sortBy(isJSONall(results[0]),["name"]);
                    let personal = (_.size(results[1]) === 0) ? [] : _.sortBy(isJSONall(results[1]),["name"]);
                    let locks = (_.size(results[2]) === 0) ? [] : _.sortBy(isJSONall(results[2]),["name"]);
                    let purchased = (_.size(results[3]) === 0) ? [] : _.sortBy(isJSONall(results[3]),["name"]);

                    return callback(null, {"personal" : personal, "basic" : basic, "purchases" : purchased, "locks" : locks});
                }
            });
    }

    static findonlybasic(req, callback) {
        //log.info('Searching items by profile ID:', req.body.profile_id);
        redis.multi()
            .hgetall(items + req.body.game_id + ":items")
            .exec(function (err, results) {
                if (err) {
                    log.error('Error while getting basic items for the game:', req.body.game_id);
                    return callback(true, {"basic" : []});
                } else {
                    //log.info('Items found:', _.size(results[0]), _.size(results[1]), _.size(results[2]));
                    let basic = (_.size(results[0]) === 0) ? [] : _.sortBy(isJSONall(results[0]),["name"]);

                    return callback(null, {"basic" : basic});
                }
            });
    }

    static create(req, callback) {
        log.info('Creating item by item names:', req.body);

        let key = (req.body.type === 'basic') ? (items + req.body.game_id + ":items") : (personal_items + req.body.profile_id + ":items");

        let id = (req.body.unique === 'true') ? nanoid.get() : req.body.name;
        req.body.timestamp = Math.floor(new Date());
        req.body.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
        req.body.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
        req.body.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');

        redis.hset(key, id, isJSON(req.body), function (err, result) {
            if (err) {
                log.error('Item cannot be stored:', value);
                return callback(true);
            } else {
                log.info('Item is created:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'created',
                    type : req.body.type,
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    context : isJSON(req.body),
                    name : id.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('items', data, function () {});

                return callback(null, req.body.name);
            }
        });
    }

    static createmultiple(req, callback) {
        log.info('Creating items by gift names:', req.body);

        let i = 0;

        _.forEach(req.body, function (value) {
            let key = (value.type === 'basic') ? (items + value.game_id + ":items") : (personal_items + value.profile_id + ":items");

            let id = (value.unique === 'true') ? nanoid.get() : value.name;
            value.timestamp = Math.floor(new Date());
            value.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
            value.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
            value.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');

            redis.hset(key, id, isJSON(value), function (err, result) {
                if (err) {
                    log.error('Item cannot be stored:', value);
                } else {
                    log.info('Item is created:', result);

                    let data = {
                        timestamp : Math.floor(new Date()),
                        profile_id : value.profile_id,
                        status : 'created',
                        type : value.type,
                        game_id : (value.game_id === undefined) ? "" : value.game_id,
                        context : isJSON(value),
                        name : value.name.toString(),
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store('items', data, function () {});
                }

                i++;

                if (i === _.size(req.body)) {
                    return callback();
                }
            });

        });
    }

    static remove(req, callback) {
        log.info('Searching items by gift name:', req.body.name);
        let key = (req.body.type === 'basic') ? (items + req.body.game_id + ":items") : (personal_items + req.body.profile_id + ":items");

        redis.hdel(key, req.body.name, function (err, result) {
            if (err) {
                log.error('Gift cannot be deleted:', req.body, err);
                return callback(true);
            } else {
                log.info('Gift is deleted:', result);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'removed',
                    type : req.body.type,
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    name : req.body.name.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('items', data, function () {});

                return callback()
            }
        });
    }

    static modify(req, callback) {
        log.info('Modifying items by item name:', req.body.name);

        let key = (req.body.type === 'basic') ? (items + req.body.game_id + ":items") : (personal_items + req.body.profile_id + ":items");

        redis.hget(key, req.body.name, function (err, result) {
            if (err) {
                log.error('Item cannot be modified:', req.body.name, err);
                return callback(true);
            } else {

                if (result !== null) {
                    let gift = JSON.parse(result);

                    if (_.size(req.body) !== 0) {

                        let i = 0;
                        _.forEach(req.body, function (value, key) {
                            _.set(gift, key, value);
                            i++;
                        });

                        if (i === _.size(req.body)) {
                            gift.timestamp = Math.floor(new Date());
                            gift.date = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
                            gift.time =  moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
                            gift.datetime = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss');

                            redis.hset(key, req.body.name, JSON.stringify(gift), function() {
                                log.info('Item is updated:', JSON.stringify(gift));

                                let data = {
                                    timestamp : Math.floor(new Date()),
                                    profile_id : req.body.profile_id,
                                    status : 'modified',
                                    type : req.body.type,
                                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                                    context : JSON.stringify(gift),
                                    name : req.body.name.toString(),
                                    date : moment(new Date()).format('YYYY-MM-DD'),
                                    time: moment(new Date()).format('HH:mm'),
                                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                };

                                Bulk.store('items', data, function () {});

                                return callback();
                            });
                        }
                    } else {
                        log.info('Nothing to update');
                        return callback();
                    }
                } else {
                    log.info('Nothing to update');
                    return callback();
                }
            }
        });
    }

    static modifymultiple(req, callback) {
        log.info('Modifying items by gift names:', req.body);

        let j = 0;

        _.forEach(req.body, function (giftvalues) {

            let key = (giftvalues.type === 'basic') ? (items + giftvalues.game_id + ":items") : (personal_items + giftvalues.profile_id + ":items");

            redis.hget(key, giftvalues.name, function (err, result) {
                if (err) {
                    log.error('Gift cannot be modified:', giftvalues, err);
                    return callback(true);
                } else {

                    if (result !== null) {
                        let gift = JSON.parse(result);

                        if (giftvalues !== 0) {

                            let i = 0;
                            _.forEach(giftvalues, function (value, key) {
                                _.set(gift, key, value);
                                i++;
                            });

                            if (i === _.size(giftvalues)) {
                                redis.hset(key, giftvalues.name, JSON.stringify(gift), function() {
                                    log.info('Gift is updated:', JSON.stringify(gift));

                                    let data = {
                                        timestamp : Math.floor(new Date()),
                                        profile_id : giftvalues.profile_id,
                                        status : 'modified',
                                        type : giftvalues.type,
                                        game_id : (giftvalues.game_id === undefined) ? "" : giftvalues.game_id,
                                        context : JSON.stringify(gift),
                                        name : giftvalues.name.toString(),
                                        date : moment(new Date()).format('YYYY-MM-DD'),
                                        time: moment(new Date()).format('HH:mm'),
                                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                    };

                                    Bulk.store('items', data, function () {});

                                    j++;

                                    if (j === _.size(req.body)) {
                                        return callback();
                                    }
                                });
                            }
                        } else {
                            log.info('Nothing to update');
                            j++;

                            if (j === _.size(req.body)) {
                                return callback();
                            }
                        }
                    } else {
                        log.info('Nothing to update');
                        j++;

                        if (j === _.size(req.body)) {
                            return callback();
                        }
                    }
                }
            });

        });
    }

    static lock(req, callback) {
        log.info('Locking gift by gift name:', req.body.name);

        redis.hset(personal_items + req.body.profile_id + ":locks", req.body.name, req.body.name, function (err, result) {
            if (err) {
                log.error('Item cannot be stored:', req.body.name, err);
            } else {
                log.info('Item is locked:', req.body.name);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'locked',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    name : req.body.name.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('items', data, function () {});

            }

            callback();
        });
    }

    static lockmultiple(req, callback) {
        log.info('Locking gift by gift names:', req.body.names);

        _.forEach(req.body.names, function (value) {
            redis.hset(personal_items + req.body.profile_id + ":locks", value, value, function (err, result) {
                if (err) {
                    log.error('Item cannot be stored:', req.body.name, err);
                } else {
                    log.info('Item is locked:', req.body.name);

                    let data = {
                        timestamp : Math.floor(new Date()),
                        profile_id : req.body.profile_id,
                        status : 'locked',
                        game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                        name : req.body.name.toString(),
                        date : moment(new Date()).format('YYYY-MM-DD'),
                        time: moment(new Date()).format('HH:mm'),
                        datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                    };

                    Bulk.store('items', data, function () {});

                }
            });
        });

        callback();
    }

    static unlock(req, callback) {
        log.info('Unlocking gift by gift name:', req.body.name);

        redis.hdel(personal_items + req.body.profile_id + ":locks", req.body.name,  function (err, result) {
            if (err) {
                log.error('Item cannot be unlocked:', req.body.name, err);
            } else {
                log.info('Item is unlocked:', req.body.name);

                let data = {
                    timestamp : Math.floor(new Date()),
                    profile_id : req.body.profile_id,
                    status : 'unlocked',
                    game_id : (req.body.game_id === undefined) ? "" : req.body.game_id,
                    name : req.body.name.toString(),
                    date : moment(new Date()).format('YYYY-MM-DD'),
                    time: moment(new Date()).format('HH:mm'),
                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                };

                Bulk.store('items', data, function () {});
            }

            callback();
        });
    }

    static unlockmultiple(req, callback) {
        log.info('Unlocking gift by gift names:', req.body.names);

        _.forEach(req.body.names, function (value) {
            redis.hdel(personal_items + req.body.profile_id + ":locks", value,  function (err, result) {
                if (err) {
                    log.error('Lock cannot be unlocked:', value, err);
                } else {
                    log.info('Item is unlocked:', value);
                }
            });
        });

        callback();
    }

    static purchase(req, callback) {
        log.info('Searching items by profile ID:', req.user.id);
        redis.hget(profiles + req.body.game_id + ":profiles", md5.md5(req.user.id), function (err, profile) {
            if (err || profile === null) {
                log.error('Profile not found for the user:', req.user.id);
                return callback(true, null);
            } else {
                redis.multi()
                    .hgetall(items + req.body.game_id + ":items")
                    .hgetall(personal_items + profile + ":items")
                    .hgetall(personal_items + profile + ":locks")
                    .hgetall(personal_items + profile + ":counters")
                    .exec(function (err, results) {
                        if (err) {
                            log.error('Error while getting items for the game:', req.user.id, profile, err);
                            return callback(true);
                        } else {
                            log.info('Items found:', _.size(results[0]), _.size(results[1]), _.size(results[2]));
                            let basic = (_.size(results[0]) === 0) ? {} : isJSONall(results[0]);
                            let personal = (_.size(results[1]) === 0) ? {} : isJSONall(results[1]);
                            let locks = (_.size(results[2]) === 0) ? {} : isJSONall(results[2]);
                            let counters = (_.size(results[3]) === 0) ? {} : isJSONall(results[3]);
                            let total_items = {"personal" : personal, "basic" : basic, "locks" : locks};

                            if (req.body.name in locks) {
                                log.info('Processing locked', req.body.type, req.body.name);
                                return callback(false, 'locked', '');
                            }

                            try {
                                let price = parseInt(_.get(total_items, [req.body.type, req.body.name, 'price']));
                                let currency = _.get(total_items, [req.body.type, req.body.name, 'currency']);
                                let balance = _.get(counters,currency);

                                if (price <= balance) {
                                    log.info('Processing purchase', req.body.type, req.body.name, price, balance);
                                    let purchase_id = nanoid.getmax();

                                    let found_item = _.get(total_items, [req.body.type, req.body.name]);

                                    let i = 0;
                                    _.forEach(req.body, function (value, key) {
                                        _.set(found_item, key, value);
                                        i++;
                                    });

                                    if (i === _.size(req.body)) {
                                        log.info('Storing new purchased item:', found_item);
                                        redis.multi()
                                            .hset(personal_items + profile + ":purchases", purchase_id, JSON.stringify(found_item))
                                            .hincrbyfloat(personal_items + profile + ":counters", currency, -(price))
                                            .exec(function (err, done) {
                                                if (err) {
                                                    log.info('Processing failed', req.body.type, req.body.name, price, balance, err);
                                                    return callback(true, 'failed','');
                                                }

                                                let data = {
                                                    timestamp : Math.floor(new Date()),
                                                    profile_id : profile,
                                                    game_id : req.body.game_id,
                                                    type : req.body.type,
                                                    name : req.body.name,
                                                    price : price.toString(),
                                                    currency : currency,
                                                    balance : done[1].toString(),
                                                    date : moment(new Date()).format('YYYY-MM-DD'),
                                                    time: moment(new Date()).format('HH:mm'),
                                                    datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                                };

                                                Bulk.store('purchases', data, function () {});

                                                callback(false, 'processed', purchase_id, done[1]);
                                            });
                                    }
                                } else {
                                    log.info("Can't purchase, not enough balance or item not found",  req.body.type, req.body.name, price, balance);
                                    callback(false, 'not_processed','');
                                }
                            } catch(err) {
                                log.info("Error, purchasing failed", err);
                                callback(true, 'failed','');
                            }

                        }
                    });
            }

        });
    }

    static purchasebyprofile(req, callback) {
        log.info('Searching items by profile ID:', req.body.profile_id);
        redis.multi()
            .hgetall(items + req.body.game_id + ":items")
            .hgetall(personal_items + req.body.profile_id + ":items")
            .hgetall(personal_items + req.body.profile_id + ":locks")
            .hgetall(personal_items + req.body.profile_id + ":counters")
            .exec(function (err, results) {
                if (err) {
                    log.error('Error while getting items for the game:', req.body.profile_id, err);
                    return callback(true);
                } else {
                    log.info('Items found:', _.size(results[0]), _.size(results[1]), _.size(results[2]));
                    let basic = (_.size(results[0]) === 0) ? {} : isJSONall(results[0]);
                    let personal = (_.size(results[1]) === 0) ? {} : isJSONall(results[1]);
                    let locks = (_.size(results[2]) === 0) ? {} : isJSONall(results[2]);
                    let counters = (_.size(results[3]) === 0) ? {} : isJSONall(results[3]);
                    let total_items = {"personal" : personal, "basic" : basic, "locks" : locks};

                    if (req.body.name in locks) {
                        log.info('Processing locked', req.body.type, req.body.name);
                        return callback(false, 'locked', '');
                    }

                    try {
                        let price = parseInt(_.get(total_items, [req.body.type, req.body.name, 'price']));
                        let currency = _.get(total_items, [req.body.type, req.body.name, 'currency']);
                        let balance = _.get(counters,currency);

                        if (price <= balance) {
                            log.info('Processing purchase', req.body.type, req.body.name, price, balance);
                            let purchase_id = nanoid.getmax();
                            let found_item = _.get(total_items, [req.body.type, req.body.name]);

                            let i = 0;
                            _.forEach(req.body, function (value, key) {
                                _.set(found_item, key, value);
                                i++;
                            });

                            if (i === _.size(req.body)) {
                                log.info('Storing new purchased item:', JSON.stringify(found_item));
                                redis.multi()
                                    .hset(personal_items + req.body.profile_id + ":purchases", purchase_id, JSON.stringify(found_item))
                                    .hincrbyfloat(personal_items + req.body.profile_id + ":counters", currency, -(price))
                                    .exec(function (err, done) {
                                        if (err) {
                                            log.info('Processing failed', req.body.type, req.body.name, price, balance, err);
                                            return callback(true, 'failed','');
                                        }

                                        let data = {
                                            timestamp : Math.floor(new Date()),
                                            profile_id : req.body.profile_id,
                                            game_id : req.body.game_id,
                                            type : req.body.type,
                                            name : req.body.name,
                                            price : price.toString(),
                                            currency : currency,
                                            balance : done[1].toString(),
                                            date : moment(new Date()).format('YYYY-MM-DD'),
                                            time: moment(new Date()).format('HH:mm'),
                                            datetime: moment(momentTimezone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
                                        };

                                        Bulk.store('purchases', data, function () {});

                                        callback(false, 'processed', purchase_id, done[1]);
                                    });
                            }
                        } else {
                            log.info("Can't purchase, not enough balance or item not found",  req.body.type, req.body.name, price, balance);
                            callback(false, 'not_processed','');
                        }
                    } catch(err) {
                        log.info("Error, purchasing failed", err);
                        callback(true, 'failed','');
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

function isJSONall(objects) {

    let object = {};
    if (_.size(objects) !== 0) {
        let i = 0;
        _.forEach(objects, function (value, key) {
            _.set(object, key, isJSONstring(value));
            i++;
        });

        if (i === _.size(objects)) {
            return object;
        }
    } else {
        return object;
    }
}

module.exports = Items;