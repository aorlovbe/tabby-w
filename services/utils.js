//Service functions
let pmx = require('pmx');
const send = require("@polka/send-type");
let probe = pmx.probe();
let redis = require('../services/redis').redisclient;
let nanoid = require('../services/nanoid');
const _ = require('lodash');
const log = require('../services/bunyan').log;

let pmx_events = probe.meter({
    name      : '[***] API Events (per sec)',
    samples   : 1
});

let pmx_redirects = probe.meter({
    name      : '[***] Redirects (shortlinks) (per sec)',
    samples   : 1
});

exports.getRandomIntInclusive = function(min, max) {
    min = Math.ceil(min);
    max = Math.floor(max);
    return Math.floor(Math.random() * (max - min + 1) + min); // The maximum is inclusive and the minimum is inclusive
}

exports.getStepEnding = function(num) {
    let last = num.toString().slice(-1);
    let ord = '';

    switch (last) {
        case '1':
            if (num.toString() === '111') {
                ord = ' ячеек';
            } else {
                ord = ' ячееку';
            }
            break;
        case '2':
            ord = ' ячейки';
            break;
        case '3':
            ord = ' ячейки';
            break;
        case '4':
            ord = ' ячейки';
            break;
        default:
            ord = ' ячеек';
            break;
    }

    return ord;

};

exports.getTriesEnding = function(num) {
    let last = num.toString().slice(-1);
    let ord = '';

    switch (last) {
        case '1':
            if (num.toString() === '11') {
                ord = ' попыток';
            } else {
                ord = ' попытка';
            }
            break;
        case '2':
            if (num.toString() === '12') {
                ord = ' попыток';
            } else {
                ord = ' попытки';
            }
            break;
        case '3':
            if (num.toString() === '13') {
                ord = ' попыток';
            } else {
                ord = ' попытки';
            }
            break;
        case '4':
            if (num.toString() === '14') {
                ord = ' попыток';
            } else {
                ord = ' попытки';
            }
            break;
        default:
            ord = ' попыток';
            break;
    }

    return ord;

};

exports.PMXmark = function (req, res, next) {
    pmx_events.mark();
    next();
}

exports.PMXmarkRedirect = function (req, res, next) {
    pmx_redirects.mark();
    next();
}

exports.decodeHTMLEntities = function (text) {
    if (typeof text === 'string') {
        let entities = [
            ['#95','_'],
            ['#x3D', '='],
            ['amp', '&'],
            ['apos', '\''],
            ['#x27', '\''],
            ['#x2F', '/'],
            ['#39', '\''],
            ['#47', '/'],
            ['lt', '<'],
            ['gt', '>'],
            ['nbsp', ' '],
            ['quot', '"'],
            ['quote', '"'],
            ['#39', "'"],
            ['#34','"']
        ];

        for (let i in entities) {
            let toreplace = '&'+entities[i][0]+';';
            text = text.replace(new RegExp(toreplace, 'g'), entities[i][1])

        }

        return text;
    } else {
        return text;
    }
}

exports.makeShort = function (url, callback) {
    let short = nanoid.get();

    redis.hset('platform:shortlinks', short, url, function (){
        callback(short);
    })
}

exports.makeLong = function (url, callback) {
    redis.hget('platform:shortlinks', url, function (err, link){
        if (err || link === null) {
            callback(null);
        } else {
            callback(link);
        }
    })
}

/*exports.generateLevelRewardsv2 = function (priority, target, history, gifts, profile, prioritySortingElements){
    log.info('Defining level gifts:', priority, target, history)

    const priorityPrizes = _.difference(priority, history); //rest of priority
    const availablePrizes = _.difference(target, history); //rest ot ЦА
    let repeatable = _.filter(gifts, { "can_be_issued_again": true, "status" : "active" }).map(item => item.id);
    let nonRepeatable = _.filter(gifts, { "can_be_issued_again": false, "status" : "active" }).map(item => item.id);
    //Фильтруем с историей (мы сначала выдаем то что еще не выигрывал человек)
    let filter_repeatable = _.difference(repeatable, history);
    let filter_nonRepeatable = _.difference(nonRepeatable, history);

    let result1 = getRandomItems(priorityPrizes, 3)
    log.info('R1', result1.length, profile, result1, '/ target:', target, '/ priority', prioritySortingElements);
    if (result1.length < 3) {
        //приоритетные + уникальные + ца
        //const targetedOrOther = _.uniq(_.concat(filter_nonRepeatable, availablePrizes));
        //let result2 =_.concat(result1, getRandomItems(targetedOrOther, (3-result1.length)));
        let availablePrioritized = getPrioritizedItems(availablePrizes, prioritySortingElements, (3-result1.length));
        let result2 =_.concat(result1, availablePrioritized);
        log.info('R2', result2.length, profile, result1, result2);

        if (result2.length < 3) {
            //let result3 = _.uniq(_.concat(result2, filter_nonRepeatable));
            //let result3_out = getRandomItems(result3,3);

            let result3 = _.difference(filter_nonRepeatable, result2); //остаток из неповторяемых
            let result3_out = _.concat(result2, getRandomItems(result3,(3-result2.length)));

            log.info('R3', result3_out.length, profile, result1, result2, result3_out);

            if (result3_out.length < 3) {
                let result4 = _.difference(repeatable, result3_out); //остаток из повторяемых
                let result4_out = _.concat(result4, getRandomItems(result4,(3-result4.length)));

                log.info('R4', profile, result1, result2, result3_out, result4_out);
                return result4_out.slice(0, 3);
            } else {
                log.info('R3');
                return result3_out.slice(0, 3);
            }
        } else {
            log.info('R2');
            return result2.slice(0, 3);
        }
    } else {
        log.info('R1');
        return result1.slice(0, 3);
    }

}*/

exports.generateLevelRewardsv2 = function (priority, target, history, gifts, profile){
    log.info('Defining level gifts:', priority, target, history)

    const priorityPrizes = _.difference(priority, history); //rest of priority
    const availablePrizes = _.difference(target, history); //rest ot ЦА
    let repeatable = _.filter(gifts, { "can_be_issued_again": true, "status" : "active" }).map(item => item.id);
    let nonRepeatable = _.filter(gifts, { "can_be_issued_again": false, "status" : "active" }).map(item => item.id);
    //Фильтруем с историей (мы сначала выдаем то что еще не выигрывал человек)
    let filter_repeatable = _.difference(repeatable, history);
    let filter_nonRepeatable = _.difference(nonRepeatable, history);

    let result1 = getRandomItems(priorityPrizes, 3);
    log.info('R1', result1.length, profile, result1);
    if (result1.length < 3) {
        //приоритетные + уникальные + ца
        //const targetedOrOther = _.uniq(_.concat(filter_nonRepeatable, availablePrizes));
        //let result2 =_.concat(result1, getRandomItems(targetedOrOther, (3-result1.length)));
        let result2 =_.concat(result1, getRandomItems(availablePrizes, (3-result1.length)));
        log.info('R2', result2.length, profile, result1, result2);

        if (result2.length < 3) {
            let result3 = _.uniq(_.concat(result2, filter_nonRepeatable));
            let result3_out = getRandomItems(result3,3);

            log.info('R3', result3_out.length, profile, result1, result2, result3_out);

            if (result3_out.length < 3) {
                let result4_out = _.concat(result3_out, getRandomItems(repeatable,(3-result3_out.length)));

                log.info('R4', profile, result1, result2, result3_out, result4_out);
                return result4_out;
            } else {
                log.info('R3');
                return result3_out;
            }
        } else {
            log.info('R2');
            return result2;
        }
    } else {
        log.info('R1');
        return result1;
    }

}


exports.generateLevelRewards = function (priority, target, history, gifts){
    //Сначала те что priority = true
    //Потом подмешиваем ЦА, те что levels (убираем special)
    log.info('Defining level gifts:', priority, target, history)

    const availablePrizes = _.difference(target, history); //rest ot ЦА
    const priorityPrizes = _.difference(priority, history); //rest of priority

    if (priorityPrizes.length >= 3) {
        //Возвращаем приоритетные
        return getRandomItems(priorityPrizes, 3);
    } else if (priorityPrizes.length === 2) {
        let repeatable = _.filter(gifts, { "can_be_issued_again": true }).map(item => item.id);
        let nonRepeatable = _.filter(gifts, { "can_be_issued_again": false }).map(item => item.id);

        //Фильтруем с историей (мы сначала выдаем то что еще не выигрывал человек)
        let filter_repeatable = _.difference(repeatable, history);
        let filter_nonRepeatable = _.difference(nonRepeatable, history);

        //If we have filter_nonRepeatable
        if (filter_nonRepeatable.length > 0) {
            const targetedOrOther = _.uniq(_.concat(filter_nonRepeatable, availablePrizes));

            let result = _.concat(getRandomItems(priorityPrizes, 2), getRandomItems(targetedOrOther, 1));
            return result;
        } else {
            if (availablePrizes.length > 0) {
                const targetedOrOther = _.uniq(_.concat(filter_nonRepeatable, availablePrizes));

                let result = _.concat(getRandomItems(priorityPrizes, 2), getRandomItems(targetedOrOther, 1));
                return result;
            } else {
                const targetedOrOther = _.uniq(_.concat(filter_repeatable, filter_nonRepeatable, availablePrizes));

                let result = _.concat(getRandomItems(priorityPrizes, 2), getRandomItems(targetedOrOther, 1));
                return result;
            }
        }

    } else if (priorityPrizes.length === 1) {
        const repeatable = _.filter(gifts, { "can_be_issued_again": true }).map(item => item.id);
        const nonRepeatable = _.filter(gifts, { "can_be_issued_again": false }).map(item => item.id);

        //Фильтруем с историей (мы сначала выдаем то что еще не выигрывал человек)
        let filter_repeatable = _.difference(repeatable, history);
        let filter_nonRepeatable = _.difference(nonRepeatable, history);

        //If we have filter_nonRepeatable
        if (filter_nonRepeatable.length > 1) {
            const targetedOrOther = _.uniq(_.concat(filter_nonRepeatable, availablePrizes));

            let result = _.concat(getRandomItems(priorityPrizes, 1), getRandomItems(targetedOrOther, 2));
            return result;
        } else {
            if (availablePrizes.length > 1) {
                const targetedOrOther = _.uniq(_.concat(filter_nonRepeatable, availablePrizes));

                let result = _.concat(getRandomItems(priorityPrizes, 1), getRandomItems(targetedOrOther, 2));
                return result;
            } else {
                const targetedOrOther = _.uniq(_.concat(filter_repeatable, filter_nonRepeatable, availablePrizes));

                let result = _.concat(getRandomItems(priorityPrizes, 1), getRandomItems(targetedOrOther, 2));
                return result;
            }
        }
    } else {
        const repeatable = _.filter(gifts, { "can_be_issued_again": true }).map(item => item.id);
        const nonRepeatable = _.filter(gifts, { "can_be_issued_again": false }).map(item => item.id);
        //Фильтруем с историей (мы сначала выдаем то что еще не выигрывал человек)
        let filter_repeatable = _.difference(repeatable, history);
        let filter_nonRepeatable = _.difference(nonRepeatable, history);

        log.info('Repeatable / non-repeatable:',filter_repeatable, filter_nonRepeatable)
        const targetedOrOther = _.uniq(_.concat(filter_repeatable, filter_nonRepeatable, availablePrizes));

        if (targetedOrOther.length < 3) {
            let repeatableHistory = _.uniq(_.concat((repeatable, history)));
            return _.concat(getRandomItems(repeatableHistory, (3-targetedOrOther.length)), getRandomItems(targetedOrOther, targetedOrOther.length));
        } else {
            return _.sampleSize(targetedOrOther,3)
        }
    }
}

function getRandomItems(arr, count) {
    return _.sampleSize(arr,count)
}

function getPrioritizedItems(arr, priorityElements, size) {
    const uniqueArr = _.shuffle(_.uniq(arr)); // Убираем дубликаты

    const prioritySelection = _.intersection(priorityElements, uniqueArr); // Выбираем элементы из приоритетного списка, которые присутствуют в массиве
    const nonPrioritySelection = _.difference(uniqueArr, prioritySelection); // Выбираем остальные элементы

    const selectedElements = _.concat(prioritySelection, nonPrioritySelection);

    return selectedElements.slice(0, size);
}