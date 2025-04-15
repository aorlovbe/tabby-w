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
}, 5000 * 60);

start();

function start() {
  sftp2
    .connect({
      host: "msftp.tabby.ai",
      port: "22",
      username: "accelera",
      password:
        "ssh-rsa AAAAB3NzaC1yc2EAAAADAQABAAACAQDGAgSAw9GFNf4ieIiU7p5VidBnWnIw9Z+1A+r8IvELZEb/dutjNzfbw53QDXnA6g9nLnXFZrlISsDQSLpwRrf1j6oU8H0//ESLV63D5NfyEe+wZkzw1C4v8QA/zU7sy0BbOFk5rNmZ8/Hc1QyFL6z2SJgIqmf58K8kfWMDcRh/4VQEX1KPdxh3pYWofMW7w4HxIN40mjSYvXZOH8XjOD4qJxzeHFIxxmQ3dzRU7xNM4p2gOLMR8t07BjdSS0kxBJiOUXVoG2dxQhaTjeqOzwPuyrkFEYUbbIQ9GBANHw88+E6zWrVfXbq8qhIHkUbLDAEoEe/spg5l5uxdvgM5riv5+Vg9PViVtLzoHy/6uJjMIzI+sWo/HAXhhYu1gX/XrTPYYKu2B9Sggn607JRzoYkFgCFAA1pslD/p4LoVslrbw+yXnRkdyZEk+ngtZ4m9XxQafkn2AGJFxcUGc0XrQspIGGs0qT2/VUW/VlA5Dk3wkkb6rCY/BpWvgb33o8VbKT7dTZQbf113lEJfANm7TE1zlbEBUyPirTdioPotkevyZHNNqaAmZqLMNTcGB+dqBtuvc9fc4+oMw2oVa0y8vhan+cVrV093ZOUmUIBpyanEr5r0Mtfah62G2l8XxDTOauQ0nrzLJKJ1XoRgWbWD3YLKX0nVH1GoKNLgDs9Ip34AvQ==",
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
                .rename(filename, "downloaded_" + filename)
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
