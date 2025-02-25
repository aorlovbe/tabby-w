let bunyan          = require('bunyan');
let PrettyStream    = require('bunyan-prettystream');
let settings        = require('../settings');
let prettyStdOut    = new PrettyStream();

prettyStdOut.pipe(process.stdout);

let log = bunyan.createLogger(
    {
        name: "api",
        level : settings.loglevel,
        streams: [{
            level : settings.loglevel,
            type: 'file',
            stream: prettyStdOut
        }]
    });

exports.log = log;