const Aerospike = require("aerospike");
const express = require("express");
const router = express.Router();
const {
  get,
  put,
  getPrev,
  scanSet,
  prevSet,
} = require("../services/aerospike");
const {
  findUser: findMongo,
  upsertUser: upsertMongo,
} = require("../services/mongo");
const {
  findUser: findYuga,
  upsertUser: upsertYuga,
} = require("../services/yuga");

const { findVitess, upsertVitess } = require("../services/vitess");

const { findScylla, upsertScylla } = require("../services/scylla");

router.post("/identity", async (req, res) => {
  const { user, table = "users" } = req.body;
  const appName = req.headers["x-app-name"];
  if (!user || !appName)
    return res.status(400).json({ error: "Missing user or app name" });

  const emailKey = `email:${appName}:${user.email}`;
  const idKey = `user:${appName}:${user.userId}`;

  try {
    const cached = (await get(idKey)) || (await get(emailKey));
    if (cached) return res.json({ user: cached });

    const result =
      appName === "rivas"
        ? await findMongo({ email: user.email }, table)
        : appName === "yuga"
        ? await findYuga(null, user.email, table)
        : appName === "vitess"
        ? await findVitess({ email: user.email }, table)
        : appName === "scylla"
        ? await findScylla({ email: user.email }, table)
        : null;

    if (result) {
      await put(idKey, result);
      await put(emailKey, result);
      return res.json({ user: result });
    }

    const newUser = {
      ...user,
      balance: user.balance || 0,
      lastSyncedAt: null,
      app: appName,
    };

    try {
      // Try to create the emailKey only if it does not exist (atomic)
      await put(emailKey, newUser, { exists: Aerospike.policy.exists.CREATE });
      // idKey can be upserted
      await put(idKey, newUser);
      return res.status(201).json({ user: newUser });
    } catch (err) {
      if (err.code === Aerospike.status.ERR_RECORD_EXISTS) {
        // Someone else just created this emailKey
        const existing = await get(emailKey);
        return res.json({ user: existing });
      }
      return res.status(500).json({ error: err.message });
    }
  } catch (err) {
    return res.status(500).json({ error: err.message });
  }
});

router.post("/sync", async (req, res) => {
  const batchSize = parseInt(req.query.batchSize || "100");
  const log = [];

  try {
    const keys = await scanSet(prevSet());
    const userKeys = keys.filter((k) => k.key.startsWith("user:"));

    for (let i = 0; i < userKeys.length; i += batchSize) {
      const batch = userKeys.slice(i, i + batchSize);
      for (const { key } of batch) {
        const user = await getPrev(key);
        if (!user || user.lastSyncedAt) continue;

        user.lastSyncedAt = new Date().toISOString();

        if (user.app === "rivas") {
          await upsertMongo(user, user.table || "users");
        } else if (user.app === "yuga") {
          await upsertYuga(user, user.table || "users");
        } else if (user.app === "vitess") {
          await upsertVitess(user, user.table || "users");
        } else if (user.app === "scylla") {
          await upsertScylla(user, user.table || "users");
        } else {
          continue;
        }

        log.push(`[SYNCED] ${user.userId}`);
      }
    }

    res.json({ message: "Sync complete", entries: log.length });
  } catch (err) {
    res.status(500).json({ error: "Sync failed", details: err.message });
  }
});

router.get("/identity/check", async (req, res) => {
  const { email, table = "users" } = req.query;
  const appName = req.headers["x-app-name"];
  if (!email || !appName) {
    return res.status(400).json({ error: "Missing email or app name" });
  }

  try {
    const emailKey = `email:${appName}:${email}`;
    let user = await get(emailKey);

    if (!user) {
      if (appName === "rivas") {
        user = await findMongo({ email }, table);
      } else if (appName === "yuga") {
        user = await findYuga(null, email, table);
      } else if (appName === "vitess" || appName === "ecommerce") {
        user = await findVitess({ email }, table);
      } else if (appName === "scylla") {
        user = await findScylla({ email }, table);
      } else {
        return res.status(400).json({ error: "Unsupported app name" });
      }
    }

    res.json({ exists: !!user, user });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
