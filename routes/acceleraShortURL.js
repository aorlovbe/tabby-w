const polka = require('polka');
const router = polka();
const send = require("@polka/send-type");
const redirect = require('@polka/redirect');
const utils = require('../services/utils');
const passport = require("../middleware/passport-auth");
const rateLimit = require("express-rate-limit");
const birthdayLimiter = rateLimit({
    windowMs: 5 * 60 * 1000, // 5 minutes
    keyGenerator: (request, response) => request.ip,
    max: 100, // Limit each IP to 5 requests per `window` (here, per 5 minutes)
    standardHeaders: true, // Return rate limit info in the `RateLimit-*` headers
    legacyHeaders: false, // Disable the `X-RateLimit-*` headers
    message: 'Too many requests, please try again later.',
    handler: (request, response, next, options) =>
        send(response, 429, {'status': 'Too many requests, please try again later.'})
})

/* Get best multiplayer node */
router.post('/', passport.authenticate('api', { session: false}), utils.PMXmarkRedirect, birthdayLimiter, (req, res, next) => {
    utils.makeLong(req.body.x, function (link){
        if (link === null) {
            return send(res, 500);
        } else {
            return send(res, 200, {"url" : link});
        }
    })
});

module.exports = router;