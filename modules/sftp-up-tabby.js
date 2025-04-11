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
  log.info("Starting CITY SFTP session / upload");
  start();
}, 1500 * 60);

start();

function start() {
  glob(
    path.join(__dirname, "../ftp/upload_to_tabby", "!(uploaded*)"),
    function (er, files) {
      if (files.length !== 0) {
        sftp
          .connect({
            host: "msftp.tabby.ai",
            port: "22",
            username: "accelera",
            password:
              "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDGAgSAw9GFNf4ieIiU7p5VidBnWnIw9Z+1A+r8IvELZEb/dutjNzfbw53QDXnA6g9nLnXFZrlISsDQSLpwRrf1j6oU8H0//ESLV63D5NfyEe+wZkzw1C4v8QA/zU7sy0BbOFk5rNmZ8/Hc1QyFL6z2SJgIqmf58K8kfWMDcRh/4VQEX1KPdxh3pYWofMW7w4HxIN40mjSYvXZOH8XjOD4qJxzeHFIxxmQ3dzRU7xNM4p2gOLMR8t07BjdSS0kxBJiOUXVoG2dxQhaTjeqOzwPuyrkFEYUbbIQ9GBANHw88+E6zWrVfXbq8qhIHkUbLDAEoEe/spg5l5uxdvgM5riv5+Vg9PViVtLzoHy/6uJjMIzI+sWo/HAXhhYu1gX/XrTPYYKu2B9Sggn607JRzoYkFgCFAA1pslD/p4LoVslrbw+yXnRkdyZEk+ngtZ4m9XxQafkn2AGJFxcUGc0XrQspIGGs0qT2/VUW/VlA5Dk3wkkb6rCY/BpWvgb33o8VbKT7dTZQbf113lEJfANm7TE1zlbEBUyPirTdioPotkevyZHNNqaAmZqLMNTcGB+dqBtuvc9fc4+oMw2oVa0y8vhan+cVrV093ZOUmUIBpyanEr5r0Mtfah62G2l8XxDTOauQ0nrzLJKJ1XoRgWbWD3YLKX0nVH1GoKNLgDs9Ip34AvQ== pub@Transmit",
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
