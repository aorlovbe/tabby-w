const Achievement       = require('../api/achievements');
const Rewards           = require('../api/rewards');
const Counter           = require('../api/counters');
const Dialog            = require('../api/dialogs');
const Task              = require('../api/tasks');
const Increment         = require('../api/increments');
const Items             = require('../api/items');
const Profile           = require('../api/profiles');
const Multiplayer       = require('../middleware/multiplayer');
const SMS = require("../middleware/sms");
const PUSH = require("../middleware/push");
const _ = require("lodash");
const settings = require("../settings");
const moment = require("moment");
const timeZone = require("moment-timezone");
const bulk = require("../services/bulk");
const Game = require("../api/games");
const {log} = require("../services/bunyan");

module.exports = {
    "push/queue" : { process(data) {
            PUSH.queue(data.body,function (err){
                if (err) return failed(data);
            });
        }},
    "push/send" : { process(data) {
            PUSH.send(data.body,function (err){
                if (err) return failed(data);
            });
        }},
    "sms/queue" : { process(data) {
            SMS.queue(data.body,function (err){
                if (err) return failed(data);
            });
        }},
    "sms/send" : { process(data) {
            SMS.send(data.body,function (err){
                if (err) return failed(data);
            });
        }},
    "items/create" : { process(data) {
            Items.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "items/modify" : { process(data) {
            Items.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "items/remove" : { process(data) {
            Items.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "achievements/create" : { process(data) {
            Achievement.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "achievements/modify" : { process(data) {
            Achievement.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "achievements/remove" : { process(data) {
            Achievement.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "rewards/create" : { process(data) {
            Rewards.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "rewards/modify" : { process(data) {
            Rewards.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "rewards/remove" : { process(data) {
            Rewards.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "counters/create" : { process(data) {
            Counter.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "counters/modify" : { process(data) {
            Counter.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "counters/remove" : { process(data) {
            Counter.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "dialogs/create" : { process(data) {
            Dialog.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "dialogs/modify" : { process(data) {
            Dialog.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "dialogs/remove" : { process(data) {
            Dialog.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "increments/create" : { process(data) {
            Increment.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "increments/modify" : { process(data) {
            Increment.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "increments/remove" : { process(data) {
            Increment.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "tasks/create" : { process(data) {
            Task.create(data, function (err){
                if (err) return failed(data);
            })
        }},
    "tasks/modify" : { process(data) {
            Task.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "tasks/remove" : { process(data) {
            Task.remove(data, function (err){
                if (err) return failed(data);
            })
        }},
    "profiles/modify" : { process(data) {
            Profile.modify(data, function (err){
                if (err) return failed(data);
            })
        }},
    "profiles/block" : { process(data) {
            let profile = data.body.profile_id;
            Profile.block(profile, function (err){
                if (err) return failed(data);
            })

        }},
    "profiles/unblock" : { process(data) {
            let profile = data.body.profile_id;

            Profile.unblock(profile, function (err){
                if (err) return failed(data);
            })

        }},
    "profiles/tags/add" : { process(data) {
            Profile.addtag(data, function (err){
                if (err) return failed(data);
            })
        }},
    "profiles/tags/remove" : { process(data) {
            Profile.removetag(data, function (err){
                if (err) return failed(data);
            })
        }}
}

function failed(fail) {
    let data = _.cloneDeep(fail.body);
    data["instance"] = settings.instance;
    data["timestamp"] = Math.floor(new Date());
    data["date"] = moment(timeZone.tz('Europe/Moscow')).format('YYYY-MM-DD');
    data["time"] = moment(timeZone.tz('Europe/Moscow')).format('HH:mm');
    data["datetime"] = moment(timeZone.tz('Europe/Moscow')._d).format('YYYY-MM-DD HH:mm:ss')
    data["context"] = JSON.stringify(fail);

    bulk.store('failed', JSON.stringify(data), function (){});
}