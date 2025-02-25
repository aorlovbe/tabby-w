let moment = require('moment');
const momentTimezone = require('moment-timezone');
let Item = require('../api/items');
let _ = require('lodash');
const log           = require('./bunyan').log;
let performance = require('performance-now');

// Определение вида Поощрения,
// одним из условий получения которого является открытие Сундука,
// осуществляется, исходя из значения РЧ1,
// рассчитанного по формуле:
// РЧ1=ОСТАТ(ОКРУГЛВВЕРХ(ОСТАТ(КОРЕНЬ(ССБ+1)*(ЗНАЧЕН(ПРАВСИМВ(ВРЗ;6))+1);10^6)/ОКРУГЛВВЕРХ(LN((ССБ+2)/( ССБ+1)); 0); 0); 10^6)*100 ,
// где:
// РЧ1 – расчетное число, используемое для определения вида Поощрения;
// ССБ – число, равное значению ZZ, полученное из XX:YY:ZZ – московское время, в которое Участником Акции была
// нажата кнопка «Открыть» для открытия Сундука,
//      где XX – часы, YY – минуты, ZZ – секунды. Если ZZ=0, тогда значение ССБ принимает значение 1;
// ВРЗ – время регистрации запроса на открытие Сундука в автоматизированной системе Организатора
// в формате ГГГГММДДччммссхххххх,
//      где ГГГГ – год, ММ – месяц, ДД – день, чч – час, мм – минута, сс – секунда, хххххх – микросекунда.

exports.getProbability = function() {
    let today = moment(momentTimezone.tz('Europe/Moscow')._d);
    let sec = today.seconds();
    let vrz = parseInt(today.milliseconds()+'000');
    sec = +sec;
    return (Math.ceil((Math.sqrt(sec+1)*(Number(vrz+1))%10**6))/Math.ceil(Math.log((sec+2)/(sec+1)))%10**6)
}

exports.getItemByProbability = function(req, callback) {
    let probability = this.getProbability();

    Item.findonlybasic(req, function (err, items) {
        if (err) return callback(null);

        let filtered = _.filter(items.basic, function (item) {
            return probability >= item.from && probability <= item.to;
        });

        log.info('Generate rewards based on basic items:', JSON.stringify(filtered[0]));
        return callback(false, filtered[0], probability);

    });
}

//TEST
function getNumber() {
    let today = moment(momentTimezone.tz('Europe/Moscow')._d);
    let sec = today.seconds();
    let vrz = parseInt(today.milliseconds()+'000');
    sec = +sec;
    return (Math.ceil((Math.sqrt(sec+1)*(Number(vrz+1))%10**6))/Math.ceil(Math.log((sec+2)/(sec+1)))%10**6)
}

function test () {
    let array = [
        {
            "id": "b-56",
            "game_id": "birthday",
            "type": "basic",
            "name": "Промокод ИМ номиналом 100 000",
            "promocode": ["promocodes-b-56"],
            "from": 999975,
            "to": 1000000
        },
        {
            "id": "b-55",
            "game_id": "birthday",
            "type": "basic",
            "name": "Промокод ИМ номиналом 30 000",
            "promocode": ["promocodes-b-55"],
            "from": 999900,
            "to": 999974
        },
        {
            "id": "b-54",
            "game_id": "birthday",
            "type": "basic",
            "name": "Промокод ИМ номиналом 10 000",
            "promocode": ["promocodes-b-54"],
            "from": 999650,
            "to": 999899
        },
        {
            "id": "b-53",
            "game_id": "birthday",
            "type": "basic",
            "name": "Толстовка",
            "promocode": ["promocodes-b-53"],
            "from": 999525,
            "to": 999649
        },
        {
            "id": "b-52",
            "game_id": "birthday",
            "type": "basic",
            "name": "Термостакан",
            "promocode": ["promocodes-b-52"],
            "from": 999275,
            "to": 999524
        },
        {
            "id": "b-51",
            "game_id": "birthday",
            "type": "basic",
            "name": "Футболка",
            "promocode": ["promocodes-b-51"],
            "from": 998650,
            "to": 999274
        },
        {
            "id": "b-50",
            "game_id": "birthday",
            "type": "basic",
            "name": "Значок",
            "promocode": ["promocodes-b-50"],
            "from": 996150,
            "to": 998649
        },
        {
            "id": "b-48",
            "game_id": "birthday",
            "type": "basic",
            "name": "Облако билайн",
            "promocode": ["promocodes-b-48"],
            "from": 946150,
            "to": 996149
        },
        {
            "id": "b-49",
            "game_id": "birthday",
            "type": "basic",
            "name": "Музыка билайн",
            "promocode": ["promocodes-b-49"],
            "from": 696150,
            "to": 946149
        },
        {
            "id": "b-00",
            "game_id": "birthday",
            "type": "basic",
            "name": "Поощрение отсутствует",
            "promocode": [],
            "from": 0,
            "to": 696149
        }
    ];

    let result = { no: 0 };
    for (let i = 0; i < 10000000; i++) {
        let probability = getNumber();
        let element = array.find(item => {
            return probability >= item.from && probability <= item.to;
        });
        if (element) {
            if (!result[element.name]) result[element.name] = 0;
            result[element.name] += 1;
        } else {
            result.no += 1;
        }
    }
    console.log('Report:', Object.keys(result).sort().reduce(
        (obj, key) => {
            obj[key] = result[key];
            return obj;
        },
        {}
    ));
}