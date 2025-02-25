let settings = require('../settings');
let log = require('../services/bunyan').log;
let Client = require('ssh2-sftp-client');
let sftp = new Client();
let fs = require('fs');
const Path = require("path");

var to_cubesolutions = settings.sftp.download;

setInterval(function () {
    log.warn('Starting SFTP session / download');
    start();
},1000*60);

start();

function start(){
    sftp.connect({
        host: settings.sftp.host,
        port: '22',
        username: settings.sftp.login,
        password: settings.sftp.pass
    }).then(() => {
        return sftp.list(to_cubesolutions);
    }).then(data => {
        if (data.length !== 0) {
            let f = 0;
            for (let i in data) {
                if (data[i].name.includes('downloaded') !== true) {
                    log.info('Downloading a file:', (f+1), data.length, data[i].name);
                    let filename = data[i].name
                    const to = Path.resolve(__dirname, '../ftp/download', filename);
                    const from = to_cubesolutions+'/'+data[i].name;
                    let destination = fs.createWriteStream(to);
                    sftp.get(from, destination);

                    destination.on("finish", function() {
                        log.info("Done writing to file %s", filename);
                        sftp.delete(from).then(() => {
                            log.warn("Deleted:", filename);
                            f++;
                        }).then(() => {
                            if (f === data.length) {
                                log.info("Done with files:", data.length);
                                sftp.end();
                            }
                        });
                    })
                } else {
                    f++;
                    if (f === data.length) {
                        log.warn("Done with files:", data.length);
                        sftp.end();
                    }
                }
            }
        } else {
            sftp.end();
        }
    }).catch(err => {
        log.error('Got SFTP error:', err);
    });
}