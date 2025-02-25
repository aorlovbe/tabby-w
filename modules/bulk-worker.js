const bulk              = require('../services/bulk');
const redis             = require('../services/redis').redisclient;
let _                   = require('lodash');
//Creating new bulk worker
//Initialize batch loaders
//  interval = 20;  //rechecking every N seconds
//  bulk = 2; //2 messages in a bulk

redis.hgetall('platform:games', function (err, games) {
    _.forEach(games, function (value, game) {
        new bulk.create(game,(20),50000);
    });

    //Additional tables addon
    new bulk.create('auth',(50),50000);

    //Additional tables addon
    new bulk.create('tasks',(70),50000);

    //Additional tables addon
    new bulk.create('dialogs',(65),50000);

    //Additional tables addon
    new bulk.create('profiles',(30),50000);

    //Additional tables addon
    new bulk.create('rewards',(45),50000);

    //Additional tables addon
    new bulk.create('matches',(73),50000);
});