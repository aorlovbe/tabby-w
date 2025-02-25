let settings = require('../settings');
let log = require('../services/bunyan').log;
let Client = require('ssh2-sftp-client');
let sftp = new Client();
let fs = require('fs');
const Path = require("path");
var glob = require("glob");
const path = require("path");
const Promise = require('bluebird');

var from_cubesolutions = settings.sftp.upload;

setInterval(function () {
    log.info('Starting SFTP session / upload');
    start();

},1500*60);

start();

function start(){
    glob(path.join(__dirname, '../ftp/upload', "!(uploaded*)"), function (er, files) {
        if (files.length !== 0) {
            sftp.connect({
                host: settings.sftp.host,
                port: '22',
                username: settings.sftp.login,
                password: settings.sftp.pass
            }).then(() => {
                log.debug("Found .csv files to upload:", files.length);

                Promise.each(files, function(file) {
                    let from = file
                    log.info('Going to upload a file:', file, from);
                    const to = from_cubesolutions + '/' + path.basename(file, '.csv');

                    sftp.put(from, to).then(() => {
                        sftp.rename(to, to + '.csv').then(() => {
                            log.warn("Renamed uploaded file to .CSV:", path.basename(file));

                            fs.rename(file, path.join(path.dirname(file),"uploaded_" + path.basename(file)), function (err) {
                                log.info('File', file, 'renamed with uploaded prefix');
                            });

                        });
                    }).catch(err => {
                        log.error('Got SFTP error:', err);
                        sftp.end();
                    });
                }).then(function(result) {
                    log.info('Finishing SFTP session',result);
                    setTimeout(function (){
                        sftp.end();
                        log.error('FTP session is over');
                    },50000)
                }).catch(function(err) {
                    log.error('Got error while processing files to unzip:', err);
                });
            })
        } else
        {
            log.info('No files to upload');
        }
    });
}