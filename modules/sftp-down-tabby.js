let settings = require("../settings");
let log = require("../services/bunyan").log;
let Client = require("ssh2-sftp-client");
let sftp2 = new Client();
let fs = require("fs");
const Path = require("path");

var to_cubesolutions = "/download_from_tabby";

setInterval(function () {
  log.warn("Starting sftp2 session for TABBY / download");
  start();
}, 3600000);

start();

function start() {
  const privateKey = fs.readFileSync("openssh_key.pem", "utf-8");
  sftp2
    .connect({
      host: "msftp.tabby.ai",
      port: "22",
      username: "accelera",
      privateKey: privateKey,
    })
    .then(() => {
      return sftp2.list(to_cubesolutions);
    })
    .then((data) => {
      log.info("Files list:", data.length);
      if (data.length !== 0) {
        let f = 0;
        for (let i in data) {
          if (data[i].name.includes("downloaded") !== true) {
            log.info("Downloading a file:", f + 1, data.length, data[i].name);
            let filename = data[i].name;
            const to = Path.resolve(
              __dirname,
              "../ftp/download_from_tabby",
              filename
            );
            const from = to_cubesolutions + "/" + data[i].name;
            let destination = fs.createWriteStream(to);
            sftp2.get(from, destination);

            destination.on("finish", function () {
              log.info("Done writing to file %s", filename);
              sftp2
                .rename(
                  from,
                  to_cubesolutions + "/" + "downloaded_" + data[i].name
                )
                .then(() => {
                  log.warn("Renamed:", filename);
                  f++;
                })
                .then(() => {
                  if (f === data.length) {
                    log.info("Done with files:", data.length);
                    sftp2.end();
                  }
                });
            });
          } else {
            f++;
            if (f === data.length) {
              log.warn("Done with files:", data.length);
              sftp2.end();
            }
          }
        }
      } else {
        sftp2.end();
      }
    })
    .catch((err) => {
      log.error("Got sftp2 error:", err);
    });
}
