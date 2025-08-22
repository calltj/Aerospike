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

router.post("/identity", async (req, res) => {
  const { user } = req.body;
  const appName = req.headers["x-app-name"];
  if (!user || !appName)
    return res.status(400).json({ error: "Missing user or app name" });

  const emailKey = `email:${user.email}`;
  const idKey = `user:${user.userId}`;

  try {
    const cached = (await get(idKey)) || (await get(emailKey));
    if (cached) return res.json({ user: cached });

    const result =
      appName === "rivas"
        ? (await findMongo({ userId: user.userId })) ||
          (await findMongo({ email: user.email }))
        : appName === "yuga"
        ? await findYuga(user.userId, user.email)
        : appName === "vitess"
        ? (await findVitess({ userId: user.userId })) ||
          (await findVitess({ email: user.email }))
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

    await put(idKey, newUser);
    await put(emailKey, newUser);

    return res.status(201).json({ user: newUser });
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
          await upsertMongo(user);
        } else if (user.app === "yuga") {
          await upsertYuga(user);
        } else if (user.app === "vitess") {
          await upsertVitess(user);
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
  const { email } = req.query;
  const appName = req.headers["x-app-name"];
  if (!email || !appName) {
    return res.status(400).json({ error: "Missing email or app name" });
  }

  try {
    let user = null;
    if (appName === "rivas") {
      user = await findMongo({ email });
    } else if (appName === "yuga") {
      user = await findYuga(null, email);
    } else if (appName === "vitess") {
      user = await findVitess({ email });
    } else {
      return res.status(400).json({ error: "Unsupported app name" });
    }

    res.json({ exists: !!user, user });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

module.exports = router;
