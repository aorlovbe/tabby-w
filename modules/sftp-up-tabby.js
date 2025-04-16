let settings = require("../settings");
let log = require("../services/bunyan").log;
let Client = require("ssh2-sftp-client");
let sftp = new Client();
let fs = require("fs");
const Path = require("path");
var glob = require("glob");
const path = require("path");
const Promise = require("bluebird");

var from_cubesolutions = "/upload_to_tabby";

setInterval(function () {
  log.info("Starting TABBY SFTP session / upload");
  start();
}, 3600000);

start();

function start() {
  glob(
    path.join(__dirname, "../ftp/upload_to_tabby", "!(uploaded*)"),
    function (er, files) {
      if (files.length !== 0) {
        const privateKey = fs.readFileSync("openssh_key", "utf-8");
        sftp
          .connect({
            host: "msftp.tabby.ai",
            port: "22",
            username: "accelera",
            privateKey: privateKey,
          })
          .then(() => {
            log.debug("Found .csv files to upload:", files.length);

            Promise.each(files, function (file) {
              let from = file;
              log.info("Going to upload a file:", file, from);
              const to = from_cubesolutions + "/" + path.basename(file, ".csv");

              sftp
                .put(from, to)
                .then(() => {
                  sftp.rename(to, to + ".csv").then(() => {
                    log.warn(
                      "Renamed uploaded file to .CSV:",
                      path.basename(file)
                    );

                    fs.rename(
                      file,
                      path.join(
                        path.dirname(file),
                        "uploaded_" + path.basename(file)
                      ),
                      function (err) {
                        log.info("File", file, "renamed with uploaded prefix");
                      }
                    );
                  });
                })
                .catch((err) => {
                  log.error("Got SFTP error:", err);
                  sftp.end();
                });
            })
              .then(function (result) {
                log.info("Finishing SFTP session:", result);
                setTimeout(function () {
                  sftp.end();
                  log.error("FTP session is over");
                }, 50000);
              })
              .catch(function (err) {
                log.error("Got error while processing files to unzip:", err);
              });
          });
      } else {
        log.info("No files to upload");
      }
    }
  );
}
