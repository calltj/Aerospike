require("../config/validateEnv");
const express = require("express");
const logger = require("./logger");
const cors = require("cors");
const cron = require("node-cron");
require("dotenv").config();
const PORT = process.env.PORT || 3000;
const {
  rotateSets,
  scanSet,
  getPrev,
  prevSet,
} = require("./services/aerospike");

const { connectAerospike, get, put } = require("./services/aerospike");
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

const {
  connectVitess,
  findVitess,
  upsertVitess,
} = require("./services/vitess");

const {
  connectScylla,
  findScylla,
  upsertScylla,
} = require("./services/scylla");

const app = express();
const rateLimit = require("express-rate-limit");
const swaggerUi = require("swagger-ui-express");
const YAML = require("yamljs");
const swaggerDocument = YAML.load("./swagger.yaml");
app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
app.use(express.json());
app.use(cors());
app.use("/api", identityRoute);

async function fullSync(batchSize = 100) {
  const { activeSet } = require("./services/aerospike").getSetNames();
  const keys = await scanSet(activeSet);
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
      } else if (user.app === "vitess") {
        await upsertVitess(user);
      } else {
        continue;
      }

      await put(key, user);
      await put(`email:${user.app}:${user.email}`, user);
      log.push(`[SYNCED] ${user.userId}`);
    }
  }

  return log;
}
cron.schedule("30 22 * * *", async () => {
  logger.info("[🧭] Rotating Aerospike sets at 10:30PM...");
  await rotateSets();
});

cron.schedule("0 23 * * *", async () => {
  logger.info("[🕛] Nightly sync started...");
  await fullSync();
});

cron.schedule("*/10 * * * *", async () => {
  logger.info("[🧠] Checking for outdated data in prevSet...");
  const keys = await scanSet(prevSet());
  for (const { key } of keys) {
    if (!key.startsWith("user:")) continue;
    const cached = await getPrev(key);
    const live =
      cached.app === "rivas"
        ? await findMongo({ userId: cached.userId })
        : cached.app === "yuga"
        ? await findYuga(cached.userId, cached.email)
        : cached.app === "vitess"
        ? await findVitess({ userId: cached.userId })
        : null;
    if (!live || JSON.stringify(live) !== JSON.stringify(cached)) {
      logger.info(`[🔄 RESYNC REQUIRED] ${cached.userId}`);
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
    await connectAerospike();
    logger.info("✅ Connected to Aerospike");
    await connectMongo();
    logger.info("✅ Connected to MongoDB");
    await connectYuga();
    logger.info("✅ Connected to YugabyteDB");
    await connectVitess();
    logger.info("✅ Connected to Vitess");
    await connectScylla();
    logger.info("✅ Connected to ScyllaDB");

    app.get("/health", (req, res) => res.json({ status: "ok" }));
    app.get("/ready", async (req, res) => {
      // Optionally check DB connections
      res.json({ ready: true });
    });
    app.use((err, req, res, next) => {
      logger.error(err);
      res.status(500).json({ error: err.message });
    });
    app.listen(PORT, () => {
      logger.info(`🚀 Identity API running on port ${PORT}`);
    });
  } catch (err) {
    logger.error("❌ Startup error:", err.message);
    process.exit(1);
  }
})();
