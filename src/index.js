const express = require("express");
const cors = require("cors");
const cron = require("node-cron");
require("dotenv").config();
const {
  rotateSets,
  scanSet,
  getPrev,
  prevSet,
} = require("./services/aerospike");

const {
//   connectAerospike,
  get,
  put,
  scanAerospikeKeys,
} = require("./services/aerospike");
const {
  connectMongo,
  findUser: findMongo,
  upsertUser: upsertMongo,
} = require("./services/mongo");
const {
  connectYuga,
  findUser: findYuga,
  upsertUser: upsertYuga,
} = require("./services/yuga");
const identityRoute = require("./routes/identity");

const app = express();
app.use(express.json());
app.use(cors());
app.use("/api", identityRoute);

async function fullSync(batchSize = 100) {
  const keys = await scanAerospikeKeys("users");
  const userKeys = keys.filter((k) => k.key.startsWith("user:"));
  const log = [];

  for (let i = 0; i < userKeys.length; i += batchSize) {
    const batch = userKeys.slice(i, i + batchSize);
    for (const { key } of batch) {
      const user = await get(key);
      if (!user || user.lastSyncedAt) continue;

      user.lastSyncedAt = new Date().toISOString();

      if (user.app === "rivas") {
        await upsertMongo(user);
      } else if (user.app === "yuga") {
        await upsertYuga(user);
      } else {
        continue;
      }

      await put(key, user);
      await put(`email:${user.email}`, user);
      log.push(`[SYNCED] ${user.userId}`);
    }
  }

  return log;
}
cron.schedule("30 22 * * *", async () => {
  console.log("[🧭] Rotating Aerospike sets at 10:30PM...");
  await rotateSets();
});

cron.schedule("0 23 * * *", async () => {
  console.log("[🕛] Nightly sync started...");
  await fullSync();
});

cron.schedule("*/10 * * * *", async () => {
  console.log("[🧠] Checking for outdated data in prevSet...");
  const keys = await scanSet(prevSet());
  for (const { key } of keys) {
    if (!key.startsWith("user:")) continue;
    const cached = await getPrev(key);
    const live =
      cached.app === "rivas"
        ? await findMongo({ userId: cached.userId })
        : cached.app === "yuga"
        ? await findYuga(cached.userId, cached.email)
        : null;
    if (!live || JSON.stringify(live) !== JSON.stringify(cached)) {
      console.log(`[🔄 RESYNC REQUIRED] ${cached.userId}`);
      cached.lastSyncedAt = new Date().toISOString();
      if (cached.app === "rivas") {
        await upsertMongo(cached);
      } else if (cached.app === "yuga") {
        await upsertYuga(cached);
      }
    }
  }
});

(async () => {
  try {
    // await connectAerospike();
    await connectMongo();
    await connectYuga();

    app.listen(5005, () => {
      console.log("✅ Identity API running on http://localhost:5005");
    });
  } catch (err) {
    console.error("❌ Startup error:", err.message);
    process.exit(1);
  }
})();
