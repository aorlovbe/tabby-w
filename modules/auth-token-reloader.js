const axios = require("axios");
const settings = require("../settings");
const log = require("../services/bunyan").log;
const redis = require("../services/redis").redisclient;
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";

log.debug("Token manager is started:", settings.beeline.payment_accounts);

setInterval(() => {
  reload();
}, 60000 * 10); //each 10 min

function reload() {
  /*    let headers = (process.env.NODE_ENV === 'development') ? {
        'Authorization' : 'Basic '+ settings.beeline.payment_accounts,
        'X-REDIRECT-TO': '73045e07-65c2-4400-aba5-1e6bcb1f4f6d'
    } : {
        'Authorization' : 'Basic '+ settings.beeline.payment_accounts
    };*/

  let headers = {
    Authorization: "Basic " + settings.beeline.payment_accounts,
  };

  axios({
    method: "GET",
    url: settings.beeline.payments + "/auth/token",
    headers: headers,
    timeout: 30000,
  })
    .then((response) => {
      log.warn(
        "[info] Authorized (refreshed) token:",
        response.data.access_token
      );
      redis.hset(
        "platform:tokens",
        "beeline-payments",
        response.data.access_token,
        function (err) {
          if (err) {
            log.error("Failed to store Beeline payment auth token");
          }
        }
      );

      return response.data.access_token;
    })
    .then((token) => {
      log.error("[info] token for sendong to cbtower:", token);
      axios.post(
        "https://cbtower.accelera.ai/api/cbtower/payment-token",
        {
          token,
        },
        {
          headers: {
            "Content-Type": "application/json",
            Authorization: "Basic YWRtaW46cGFzc3dvcmQ=",
          },
        }
      );
    })
    .catch((err) => {
      log.error(
        "Failed to get Beeline payment auth token:",
        settings.beeline.payment_accounts,
        err
      );
    });
}

reload();
