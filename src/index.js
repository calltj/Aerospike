const express = require("express");
const logger = require("./logger");
const cors = require("cors");
const cron = require("node-cron");
require("dotenv").config();
require("../config/validateEnv");
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
} = require("./services/mongo");
const {
  connectYuga,
  findUser: findYuga,
} = require("./services/yuga");
const identityRoute = require("./routes/identity");
const {
  connectVitess,
  findVitess,
} = require("./services/vitess");
const {
  connectScylla,
  findScylla,
} = require("./services/scylla");

const app = express();
app.set("trust proxy", 1);
const rateLimit = require("express-rate-limit");
const swaggerUi = require("swagger-ui-express");
const YAML = require("yamljs");
const swaggerDocument = YAML.load("./swagger.yaml");
app.use("/docs", swaggerUi.serve, swaggerUi.setup(swaggerDocument));
app.use(rateLimit({ windowMs: 15 * 60 * 1000, max: 100 }));
app.use(express.json());
app.use(cors());
app.use("/api", identityRoute);

// Only rotate Aerospike sets, no sync to other DBs
cron.schedule("27 11 * * *", async () => {
  logger.info("[] Rotating Aerospike sets at 10:30PM...");
  await rotateSets();
});

cron.schedule("*/10 * * * *", async () => {
  logger.info("[] Checking for outdated data in prevSet...");
  const keys = await scanSet(prevSet());
  for (const { key } of keys) {
    if (!key.startsWith("user:")) continue;
    
  }
});

(async () => {
  try {
    await connectAerospike();
    logger.info(" Connected to Aerospike");
    await connectMongo();
    logger.info(" Connected to MongoDB");
    await connectYuga();
    logger.info(" Connected to YugabyteDB");
    await connectVitess();
    logger.info(" Connected to Vitess");
    await connectScylla();
    logger.info(" Connected to ScyllaDB");

    app.get("/health", (req, res) => res.json({ status: "ok" }));
    app.get("/ready", async (req, res) => {
      
      res.json({ ready: true });
    });
    app.use((err, req, res, next) => {
      logger.error(err);
      res.status(500).json({ error: err.message });
    });
    app.listen(PORT, () => {
      logger.info(` Identity API running on port ${PORT}`);
    });
  } catch (err) {
    logger.error("❌ Startup error:", err);
    process.exit(1);
  }
})();