const passport = require('passport');
const HeaderAPIKeyStrategy = require('passport-headerapikey').HeaderAPIKeyStrategy;
const BasicStrategy = require('passport-http').BasicStrategy;
const Profile = require('../api/profiles');
const User = require('../api/users');
const redis = require('../services/redis').redisclient;
const log = require('../services/bunyan').log;

passport.use('api', new HeaderAPIKeyStrategy({ header: 'Authorization', prefix: 'Key ' }, true,
    function(hash, done, req) {
        findByUserHash(hash, function(err, user) {
            if (err) {
                req.body.key = "failed";
                return done(err);
            }
            if (!user) {
                req.body.key = "failed";
                return done(null, false, { message: 'Unknown user ' + user });
            }
            req.body.key = hash;
            req.body.system = user.system;
            req.body.game_id = (user.system === 'Management') ? req.body.game_id : user.system;
            req.body.responsible = user.responsible;
            return done(null, user);
        })
    }
));

passport.use('management', new BasicStrategy(
    function(userid, password, done) {
        if (userid === process.env.BASIC_AUTH_USER &&
            password === process.env.BASIC_AUTH_PASSWORD) return done(null, {user: process.env.BASIC_AUTH_USER});
        return done(true);
    }
));

function findByUserHash(hash, callback) {
    redis.hget('platform:api', hash, function (err, result) {
        if (err || result === null) {
            log.error('Accelera Game API request was rejected, key:', hash);
            return callback(null);
        } else {
            return callback(null, JSON.parse(result));
        }
    });
}

passport.serializeUser(function (user, callback) {
    callback(null, user);
});

passport.deserializeUser(function (id, callback) {
    User.find(id, callback);
});

module.exports = passport;