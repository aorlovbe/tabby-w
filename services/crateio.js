const Pool = require('pg').Pool
const log               = require('./bunyan').log;

const pool = new Pool({
    user: 'crate',
    host: 'localhost',
    database: 'doc',
    password: '',
    max: 100,
    port: 5432,
})

const getRewardsByMetka = async function (metka, callback) {
    let connection = await pool.connect();

    try {
        let select = `select * from birthday where metka = '${metka}' limit 1`
        const results = await connection.query(select);
        callback(false, results.rows[0])
    } catch (e) {
        log.error('Got error while selecting by metka:', e, metka);
        callback(true)
    } finally {
        await connection.release();
    }
}

const getRewardsByCTN = async function (ctn, callback) {
    let connection = await pool.connect();

    try {
        let select = `select * from city_september2024 where ctn = '${ctn}' limit 1`
        const results = await connection.query(select);
        callback(false, results.rows[0])
    } catch (e) {
        log.error('Got error while selecting by ctn:', e, ctn);
        callback(true)
    } finally {
        await connection.release();
    }
}

const getXMAS2023RewardsByCTN = async function (ctn, callback) {
    let connection = await pool.connect();

    try {
        let select = `select * from xmas2023_december_v2 where ctn = '${ctn}' limit 1`
        const results = await connection.query(select);
        callback(false, results.rows[0])
    } catch (e) {
        log.error('Got error while selecting by ctn / XMAS2023:', e, ctn);
        callback(true)
    } finally {
        await connection.release();
    }
}

const getXMAS2023SegmentByCTN = async function (ctn, callback) {
    let connection = await pool.connect();

    try {
        let select = `select segment from xmas2023_segments_v3 where ctn = '${ctn}' limit 1`
        const results = await connection.query(select);
        callback(false, results.rows[0])
    } catch (e) {
        log.error('Got error while selecting segments by ctn / XMAS2023:', e, ctn);
        callback(true)
    } finally {
        await connection.release();
    }
}

const getBirthdayRewardsByCTN = async function (ctn, callback) {
    let connection = await pool.connect();

    try {
        let select = `select * from birthday_october_v3 where ctn = '${ctn}' limit 1`
        const results = await connection.query(select);
        callback(false, results.rows[0])
    } catch (e) {
        log.error('Got error while selecting by ctn:', e, ctn);
        callback(true)
    } finally {
        await connection.release();
    }
}

const getAdditionalRewardsByCTN = async function (ctn, callback) {
    let connection = await pool.connect();

    try {
        let select = `select * from city_timelimited where ctn = '${ctn}' limit 1`
        const results = await connection.query(select);
        callback(false, results.rows[0])
    } catch (e) {
        log.error('Got error while selecting by ctn:', e, ctn);
        callback(true)
    } finally {
        await connection.release();
    }
}
exports.getRewardsByMetka = getRewardsByMetka;
exports.getAdditionalRewardsByCTN = getAdditionalRewardsByCTN;
exports.getRewardsByCTN = getRewardsByCTN;
exports.getBirthdayRewardsByCTN = getBirthdayRewardsByCTN;
exports.getXMAS2023RewardsByCTN = getXMAS2023RewardsByCTN;
exports.getXMAS2023SegmentByCTN = getXMAS2023SegmentByCTN;
