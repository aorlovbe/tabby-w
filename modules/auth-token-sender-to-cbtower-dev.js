const axios = require("axios");
const settings = require("../settings");
const log = require("../services/bunyan").log;
const redis = require("../services/redis").redisclient;
process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";

const url_dev = "https://g1-dev.accelera.ai/v1/api/cbtower/payment-token";
const url_prod = "https://cbtower.accelera.ai/api/cbtower/payment-token";

setInterval(() => {
  reload();
}, 60000 * 2); //each 10 min

function reload() {
  redis.hget(
    "platform:tokens",
    "beeline-payments",
    async function (err, token) {
      if (err) {
        log.error("Failed to store Beeline payment auth token");
      }
      try {
        await axios.post(
          url_prod,
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
        log.info("token:", token, "was send to:", url_dev);
      } catch (error) {
        log.error(error);
      }
    }
  );
}

reload();
