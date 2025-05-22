let settings = require("../settings");
let log = require("../services/bunyan").log;
let Client = require("ssh2-sftp-client");
let sftp2 = new Client();
let fs = require("fs");
const Path = require("path");

const to_cubesolutions = "/download_from_tabby";

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
    .then(() => sftp2.list(to_cubesolutions))
    .then((data) => {
      log.info("Files list:", data.length);
      if (data.length !== 0) {
        let f = 0;
        for (let i in data) {
          const filename = data[i].name;
          if (!filename.startsWith("downloaded_")) {
            log.info("Downloading file:", f + 1, data.length, filename);
            const from = `${to_cubesolutions}/${filename}`;
            const localPath = Path.resolve(
              __dirname,
              "../ftp/download_from_tabby",
              filename
            );
            const destination = fs.createWriteStream(localPath);

            sftp2.get(from, destination);

            destination.on("finish", function () {
              log.info("Finished writing file:", filename);

              // Переименование на сервере
              sftp2
                .rename(from, `${to_cubesolutions}/downloaded_${filename}`)
                .then(() => {
                  log.warn("Remote file renamed:", filename);

                  // Переименование локального файла в ready_*
                  const readyLocalPath = Path.resolve(
                    __dirname,
                    "../ftp/download_from_tabby",
                    `ready_${filename}`
                  );
                  fs.rename(localPath, readyLocalPath, (err) => {
                    if (err) {
                      log.error("Local rename error:", err);
                    } else {
                      log.info(
                        "Local file renamed to ready_*:",
                        readyLocalPath
                      );
                    }

                    f++;
                    if (f === data.length) {
                      log.info("All files processed:", data.length);
                      sftp2.end();
                    }
                  });
                })
                .catch((err) => {
                  log.error("Remote rename error:", err);
                  f++;
                  if (f === data.length) {
                    log.info("All files processed:", data.length);
                    sftp2.end();
                  }
                });
            });

            destination.on("error", function (err) {
              log.error("Write stream error for file:", filename, err);
              f++;
              if (f === data.length) {
                log.info("All files processed (with errors):", data.length);
                sftp2.end();
              }
            });
          } else {
            f++;
            if (f === data.length) {
              log.warn("No new files to process.");
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
