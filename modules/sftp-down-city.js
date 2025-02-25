let settings = require('../settings');
let log = require('../services/bunyan').log;
let Client = require('ssh2-sftp-client');
let sftp2 = new Client();
let fs = require('fs');
const Path = require("path");

var to_cubesolutions = '/cubesolutions/city/to_CUBESOLUTIONS';

setInterval(function () {
    log.warn('Starting sftp2 session for CITY / download');
    start();
},5000*60);

start();

function start(){
    sftp2.connect({
        host: '217.118.84.207',
        port: '22',
        username: 'cubesolutions_part',
        password: 'qhhq4XsUck9K'
    }).then(() => {
        return sftp2.list(to_cubesolutions);
    }).then(data => {
        log.info('Files list:', data.length );
        if (data.length !== 0) {
            let f = 0;
            for (let i in data) {
                if (data[i].name.includes('downloaded') !== true) {
                    log.info('Downloading a file:', (f+1), data.length, data[i].name);
                    let filename = data[i].name
                    const to = Path.resolve(__dirname, '../ftp/download_city', filename);
                    const from = to_cubesolutions+'/'+data[i].name;
                    let destination = fs.createWriteStream(to);
                    sftp2.get(from, destination);

                    destination.on("finish", function() {
                        log.info("Done writing to file %s", filename);
                        sftp2.delete(from).then(() => {
                            log.warn("Deleted:", filename);
                            f++;
                        }).then(() => {
                            if (f === data.length) {
                                log.info("Done with files:", data.length);
                                sftp2.end();
                            }
                        });
                    })
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
    }).catch(err => {
        log.error('Got sftp2 error:', err);
    });
}