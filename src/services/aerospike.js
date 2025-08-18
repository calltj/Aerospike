const Aerospike = require('aerospike');
const { DateTime } = require('luxon');
require('dotenv').config();
console.log('AEROSPIKE_HOST:', process.env.AEROSPIKE_HOST);
console.log('AEROSPIKE_PORT:', process.env.AEROSPIKE_PORT);
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
let activeSet = '';
let prevSet = '';

function getSetNames() {
  const now = DateTime.now().setZone('Africa/Lagos');
  const today = now.startOf('day');
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

  console.log(`[🧭] Active Set: ${activeSet}, Prev Set: ${prevSet}`);
}

async function connectAerospike() {
  await client.connect();
  getSetNames();
  console.log('✅ Connected to Aerospike');
}

function get(key) {
  return client.get(new Aerospike.Key('test', activeSet, key)).then(res => res.bins).catch(() => null);
}

function put(key, data) {
  return client.put(new Aerospike.Key('test', activeSet, key), data);
}

function getPrev(key) {
  return client.get(new Aerospike.Key('test', prevSet, key)).then(res => res.bins).catch(() => null);
}

function scanSet(setName) {
  return new Promise((resolve, reject) => {
    const scan = client.scan('test', setName);
    const keys = [];
    scan.foreach(
      record => {
        keys.push({ key: record.key.key });
      },
      err => {
        if (err) reject(err);
        else resolve(keys);
      }
    );
  });
}

async function rotateSets() {
  const twoDaysAgo = DateTime.now().setZone('Africa/Lagos').minus({ days: 2 }).toISODate();
  const oldSet = `users_${twoDaysAgo}`;
  console.log(`[🧹] Rotating sets. Deleting old set: ${oldSet}`);

  const keys = await scanSet(oldSet);
  for (const { key } of keys) {
    await client.remove(new Aerospike.Key('test', oldSet, key)).catch(() => {});
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