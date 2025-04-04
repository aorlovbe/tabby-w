const axios = require("axios");
const log = require("../services/bunyan").log;
const send = require("@polka/send-type");

const getUserInfo = async (target, client, gameid) => {
  const result = axios({
    method: "GET",
    url: `https://tabby-api.accelera.ai/v1/internal/${target}/clients/${client}/${gameid}`,
    headers: {
      "Content-Type": "application/json",
    },
    timeout: 30000,
  })
    .then((response) => {
      console.log("THERE1");
      return response.data[target];
    })
    .catch((err) => {
      log.error(`Error with ${target} info`, err);
    });

  return result;
};

const createUserReward = async (rewardId, client, gameid) => {
  const reward = axios({
    method: "POST",
    url: `https://tabby-api.accelera.ai/v1/internal/rewards/clients`,
    headers: {
      "Content-Type": "application/json",
    },
    data: {
      client_id: client,
      environment_id: gameid,
      id: rewardId,
    },
    timeout: 30000,
  })
    .then((response) => {
      return response.data;
    })
    .catch((err) => {
      log.error(`Error with creating reward info`, err);
    });
  return reward;
};

const createUserElement = async (elementId, client, gameid, collectionId) => {
  const reward = axios({
    method: "POST",
    url: `https://tabby-api.accelera.ai/v1/internal/collections/clients`,
    headers: {
      "Content-Type": "application/json",
    },
    data: {
      client_id: client,
      environment_id: gameid,
      id: collectionId,
      element_id: elementId,
    },
    timeout: 30000,
  })
    .then((response) => {
      return response.data;
    })
    .catch((err) => {
      log.error(`Error with creating reward info`, err);
    });

  return reward;
};

const purchaseChest = async (rewardId, client, gameid) => {
  const reward = axios({
    method: "POST",
    url: `https://tabby-api.accelera.ai/v1/internal/wallet/purchase`,
    headers: {
      "Content-Type": "application/json",
    },
    data: {
      client_id: client,
      environment_id: gameid,
      id: rewardId,
    },
    timeout: 30000,
  })
    .then((response) => {
      "response.data.id", response.data;
      return response.data;
    })
    .catch((err) => {
      log.error(`Error with creating reward info`, err);
    });

  return reward;
};

const openChest = async (rewardId, client, gameid) => {
  const reward = axios({
    method: "POST",
    url: `https://tabby-api.accelera.ai/v1/internal/items/treasure/open`,
    headers: {
      "Content-Type": "application/json",
    },
    data: {
      client_id: client,
      environment_id: gameid,
      id: rewardId,
    },
    timeout: 30000,
  })
    .then((response) => {
      if (response.status === 'status": "Failed to open') {
        return send(res, 500, { status: "failed" });
      }
      return response.data;
    })
    .catch((err) => {
      log.error(`Error with creating reward info`, err);
    });
  console.log("THERE6");

  return reward;
};

const getUserRewards = async (gameid) => {
  const rewards = axios({
    method: "GET",
    url: `https://tabby-api.accelera.ai/v1/internal/rewards/environments/${gameid}`,
    headers: {
      "Content-Type": "application/json",
    },
    timeout: 30000,
  })
    .then((response) => {
      return response.data.rewards;
    })
    .catch((err) => {
      log.error(`Error with getting rewards info`, err);
    });

  return rewards;
};

const getCollectionsInfo = async (client) => {
  const tasks = axios({
    method: "GET",
    url: `https://tabby-api.accelera.ai/v1/internal/collections/entities/${client}`,
    headers: {
      "Content-Type": "application/json",
    },
    timeout: 30000,
  })
    .then((response) => {
      return response.data.achievement;
    })
    .catch((err) => {
      log.error(`Error with ${target} info`, err);
    });

  return tasks;
};

module.exports = {
  getUserInfo,
  createUserReward,
  getUserRewards,
  purchaseChest,
  createUserElement,
  getCollectionsInfo,
  openChest,
};
