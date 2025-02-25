const polka = require('polka');
const router = polka();
const passport = require("../middleware/passport-auth");
const log = require("../services/bunyan").log;
const API               = require('../middleware/api');
const Token             = require('../middleware/tokens');
const Profiles          = require('../api/profiles');
const Leaderboard       = require('../api/leaderboard');
const Nakama       = require('../middleware/nakama');
const send = require("@polka/send-type");
const producer = require('../services/producer');

/* Get best multiplayer node */
router.post('/auth', passport.authenticate('api', { session: false}),  (req, res, next) => {
    log.info('Got auth message from Nakama instance:', JSON.stringify(req.body));

    //{"status":"signin","profile_id":"ivan","userId":"df60f5f6-87c7-42ce-a570-59537ea91062","key":"noPOc2KY8Hub8_kB4xt_DT4cwPdyqj9IVWsXXqSx","system":"rock-paper-scissors","game_id":"rock-paper-scissors","responsible":"mk@cubesolutions.ru"}
    send(res, 200, {"status" : 'ok'});

    //Send event as signin/signup from Nakama auth
    API.publish(req.body.profile_id, 'session', req.body, function (){})

});

router.post('/match', passport.authenticate('api', { session: false}), Nakama.processMatchData,  (req, res, next) => {
    log.info('Got match message from Nakama instance:', JSON.stringify(req.body));

    send(res, 200, {"status" : 'ok'});
});

//Getting match rewards by probability, match is started
router.post('/rewards', passport.authenticate('api', { session: false}), Nakama.getMatchRewards,  (req, res, next) => {
    log.info('Got match rewards request for Nakama instance:', JSON.stringify(req.body));

    send(res, 200, {"status" : 'ok', "rewards" : req.body.rewards});

});

//Getting match rewards by probability, match is started
router.post('/antifraudCheckup', passport.authenticate('api', { session: false}), Nakama.AntifraudCheckup, (req, res, next) => {
    log.info('Got antifraud checkup request for Nakama instance:', JSON.stringify(req.body));
    log.info('Final antifraud reply is:',  req.body.decision, "match", req.body.match);
    send(res, 200, {"status" : 'ok', "decision" : req.body.decision, "match" : req.body.match});

});

//Match joins info for antifraud
router.post('/joins', passport.authenticate('api', { session: false}), Nakama.setMatchJoins, (req, res, next) => {

    send(res, 200, {"status" : 'ok'});

});

module.exports = router;