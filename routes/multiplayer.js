const polka = require('polka');
const router = polka();
const passport = require("../middleware/passport-auth");
const log = require("../services/bunyan").log;
const API               = require('../middleware/api');
const Token             = require('../middleware/tokens');
const Profiles             = require('../api/profiles');
const Multiplayer       = require('../middleware/multiplayer');
const send = require("@polka/send-type");
const producer = require('../services/producer');

/* Get best multiplayer node */
router.post('/nodes', passport.authenticate('api', { session: false}), API.getGame, Token.Decrypt, Multiplayer.getAvailableNodes, API.isBlocked, API.isBanned, API.reloadDailyLeaderboard, (req, res, next) => {
    send(res, 200, {status: 'ok', "nodes" : req.body.nodes});

    API.publish(req.body.profile_id, 'nodes', req.body.decrypted_token, function (){})
});

/* Set game avatar */
router.post('/avatar/update', passport.authenticate('api', { session: false}), API.getGame, Token.Decrypt, Multiplayer.updateAvatarOnNodes, (req, res, next) => {
    Profiles.modify({"body" : {
            "profile_id" : req.body.profile_id,
            "avatar" : req.body.avatar,
            "game_id" : req.body.game.game_id
    }}, function (err, ok){
        if (err) return send(res, 500, {"status" : 'failed'});
        req.body.decrypted_token.avatar = req.body.avatar;

        API.publish(req.body.profile_id, 'avatar', req.body.decrypted_token, function (){})

        send(res, 200, {"status" : 'updated', "avatar" : req.body.avatar});
    })
});

/* Check multiplayer events from frontend */
router.post('/events', passport.authenticate('api', { session: false}), API.getGame, Token.Decrypt, (req, res, next) => {
    send(res, 200, {"status" : "ok"});
    producer.publishTrigger(req.body.profile_id, req.body.event, req.body.context);
});

module.exports = router;