const Aerospike = require("aerospike");
const { DateTime } = require("luxon");
require("dotenv").config();
const logger = require("../logger");

logger.info("AEROSPIKE_HOST:", process.env.AEROSPIKE_HOST);
logger.info("AEROSPIKE_PORT:", process.env.AEROSPIKE_PORT);

const config = {
  hosts: [
    {
      addr: process.env.AEROSPIKE_HOST,
      port: Number(process.env.AEROSPIKE_PORT),
    },
  ],
  log: { level: Aerospike.log.INFO },
};

const client = Aerospike.client(config);
let activeSet = "";
let prevSet = "";

// Default namespace if not provided (should be overridden by passing namespace)
const DEFAULT_NAMESPACE = process.env.AEROSPIKE_NAMESPACE || "ecommerce";

// Determines the active and previous set names based on time
function getSetNames() {  
  const now = DateTime.now().setZone("Africa/Lagos");
  const today = now.startOf("day");
  const rotationHour = 22;
  const rotationMinute = 30;

  let activeDate, prevDate;

  if (
    now.hour > rotationHour ||
    (now.hour === rotationHour && now.minute >= rotationMinute)
  ) {
    activeDate = today.plus({ days: 1 });
    prevDate = today;
  } else {
    activeDate = today;
    prevDate = today.minus({ days: 1 });
  }

  activeSet = `users_${activeDate.toISODate()}`;
  prevSet = `users_${prevDate.toISODate()}`;

  logger.info(`[] Active Set: ${activeSet}, Prev Set: ${prevSet}`);
}

async function connectAerospike() {
  await client.connect();
  getSetNames();
  logger.info(" Connected to Aerospike");
}

// Always pass the namespace for multi-app support!
function get(key, namespace = DEFAULT_NAMESPACE) {
  return client
    .get(new Aerospike.Key(namespace, activeSet, key))
    .then((res) => res.bins)
    .catch(() => null);
}

function put(key, data, policy = {}, namespace = DEFAULT_NAMESPACE) {
  const options = Object.keys(policy).length ? { policy } : {};
  return client.put(new Aerospike.Key(namespace, activeSet, key), data, options);
}

function getPrev(key, namespace = DEFAULT_NAMESPACE) {
  return client
    .get(new Aerospike.Key(namespace, prevSet, key))
    .then((res) => res.bins)
    .catch(() => null);
}

function scanSet(setName, namespace = DEFAULT_NAMESPACE) {
  return new Promise((resolve, reject) => {
    const scan = client.scan(namespace, setName);
    const keys = [];
    scan.foreach(
      (record) => {
        keys.push({ key: record.key.key });
      },
      (err) => {
        if (err) reject(err);
        else resolve(keys);
      }
    );
  });
}

async function rotateSets(namespace = DEFAULT_NAMESPACE) {
  const twoDaysAgo = DateTime.now()
    .setZone("Africa/Lagos")
    .minus({ days: 2 })
    .toISODate();
  const oldSet = `users_${twoDaysAgo}`;
  logger.info(`[] Rotating sets. Deleting old set: ${oldSet}`);

  const keys = await scanSet(oldSet, namespace);
  for (const { key } of keys) {
    await client.remove(new Aerospike.Key(namespace, oldSet, key)).catch(() => {});
  }

  getSetNames(); // Refresh active and prev sets
}

module.exports = {
  connectAerospike,
  get,
  put,
  getPrev,
  scanSet,
  rotateSets,
  activeSet: () => activeSet,
  prevSet: () => prevSet,
};