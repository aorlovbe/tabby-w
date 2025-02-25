const polka = require("polka");
const helmet = require("helmet");
const cors = require("cors");
const compression = require("compression");
const send = require("@polka/send-type");
const bodyParser = require("body-parser");
const settings = require("./settings");
const log = require("./services/bunyan").log;
const _ = require("lodash");
const producer = require("./services/producer");
const consumer = require("./services/consumer");
const messages = require("./routes/messages");
const api = require("./routes/api");
const tabbyW = require("./routes/tabbyW");
const xmas2023 = require("./routes/xmas2023");
const birthday = require("./routes/birthday-2023");
const management = require("./routes/management");
const multiplayer = require("./routes/multiplayer");
const shortURL = require("./routes/acceleraShortURL");
const nakama = require("./routes/nakama");
const utils = require("./services/utils");

process.env["NODE_TLS_REJECT_UNAUTHORIZED"] = "0";

const app = polka().listen(settings.server.port, () => {
  log.info("Accelera Game API is listening on port", settings.server.port);
});

app.use(helmet());
app.use(
  cors({
    origin: "*",
    methods: "GET,HEAD,PUT,PATCH,POST,DELETE",
  })
);

app.use(bodyParser.json({ limit: "250mb" }));
app.use(bodyParser.urlencoded({ limit: "250mb", extended: true }));

app.use(compression());
/* Game API routers for games & game management */
app.use("/api", utils.PMXmark, api);
app.use("/tabbyW", utils.PMXmark, tabbyW);
app.use("/management", utils.PMXmark, management);
app.use("/multiplayer", multiplayer);
app.use("/nakama", nakama);
app.use("/x", shortURL);

/* ------------------------------------------------------------- */
/* Accelera Flows triggers producer and Game API events consumer */
producer.createProducer(settings.instance).then(function () {
  log.info("Accelera Game API producer is created:", settings.instance);
  // producer.publishGameEvent(settings.instance, {"type" : "sms/send", "target" : "+79037446376", "message" : "Тест"}).then(function (){
  //     log.info('Message was published')
  // });
});

/* ------------------------------------------------------------- */
/* Create games consumer for Accelera Flows events */
consumer.createConsumer(settings.instance, async (msg) => {
  let message = JSON.parse(Buffer.from(msg.content));
  log.debug("Incoming message:", JSON.stringify(message));
  try {
    // Endpoint is like ingame/message, counters/create etc.
    // ingame/message can have sms : true for sms
    if (message.endpoint === "ingame/message") {
      if (message.type === "ingame/queue") {
        //log.warn('Queuing SMS from bulk:', message);
        messages["sms/queue"].process({ body: message });
      } else if (message.type === "ingame/push-queue") {
        log.info("Queuing Push:", message);
        messages["push/queue"].process({ body: message });
      } else if (message.type === "ingame/push-send") {
        log.info("Sending Push:", message);
        messages["push/send"].process({ body: message });
      } else {
        log.info("Sending SMS from flow:", message);
        messages["sms/send"].process({ body: message });
      }
    } else {
      messages[message.endpoint].process({ body: message });
    }
  } catch (e) {
    log.error("Failed to process message from the queue:", e, message);
  }
  consumer.ack(msg);
});
