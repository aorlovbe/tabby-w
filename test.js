const csvToJson = require("csvtojson");
const Tasks = require("./api/tasks");
const Profiles = require("./api/profiles");
const { publishTrigger } = require("./services/producer");
const { log } = require("./services/bunyan");

const processRecipients = async () => {
  const arr = [];
  const recipients = await csvToJson({
    trim: true,
  }).fromFile("./customer.csv");

  recipients.forEach((recipient) => {
    arr.push(recipient);
  });
  return arr;
};

const process = async () => {
  const arr = await processRecipients();
  arr.forEach((el) => {
    const req = {
      body: {
        id: el.client_id,
      },
    };

    const profile_id = Profiles.findbyuser(req);


};
