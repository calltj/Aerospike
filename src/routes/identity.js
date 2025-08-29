const Aerospike = require("aerospike");
const express = require("express");
const router = express.Router();
const authMiddleware = require("../middleware/auth");
const logger = require("../logger");
const { compare } = require("../utils/password");
const jwt = require("jsonwebtoken");
const SECRET = process.env.JWT_SECRET || "supersecret";
const { hash } = require("../utils/password");
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
  logger.info("Received POST /identity", req.body, req.headers["x-app-name"]);
  const { user, table = "users" } = req.body;
  const appName = req.headers["x-app-name"];
  if (!user || !appName)
    return res.status(400).json({ error: "Missing user or app name" });
  if (user.password) {
    user.hashedPassword = await hash(user.password);
    delete user.password; 
  }
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

router.post("/identity/login", async (req, res) => {
  const { email, password, table = "users" } = req.body;
  const appName = req.headers["x-app-name"];
  if (!email || !password || !appName)
    return res.status(400).json({ error: "Missing email, password, or app name" });

  // Fetch user from the correct DB
  let userFromDb = null;
  if (appName === "rivas") {
    userFromDb = await findMongo({ email }, table);
  } else if (appName === "yuga") {
    userFromDb = await findYuga(null, email, table);
  } else if (appName === "vitess" || appName === "ecommerce") {
    userFromDb = await findVitess({ email }, table);
  } else if (appName === "scylla") {
    userFromDb = await findScylla({ email }, table);
  } else {
    return res.status(400).json({ error: "Unsupported app name" });
  }

  if (!userFromDb) return res.status(401).json({ error: "User not found" });

  // Compare password
  const isMatch = await compare(password, userFromDb.hashedPassword);
  if (!isMatch) return res.status(401).json({ error: "Invalid password" });

  // Issue JWT
  const token = jwt.sign(
    { userId: userFromDb.userId, email: userFromDb.email, app: appName },
    SECRET,
    { expiresIn: "1d" }
  );

  res.json({ token, user: userFromDb });
});

router.put("/identity", authMiddleware, async (req, res) => {
  const { userId, updates, table = "users" } = req.body;
  const appName = req.headers["x-app-name"];
  if (!userId || !updates || !appName)
    return res.status(400).json({ error: "Missing userId, updates, or app name" });

  let updatedUser = null;
  try {
    if (appName === "rivas") {
      updatedUser = await upsertMongo({ ...updates, userId }, table);
    } else if (appName === "yuga") {
      updatedUser = await upsertYuga({ ...updates, userId }, table);
    } else if (appName === "vitess" || appName === "ecommerce") {
      updatedUser = await upsertVitess({ ...updates, userId }, table);
    } else if (appName === "scylla") {
      updatedUser = await upsertScylla({ ...updates, userId }, table);
    } else {
      return res.status(400).json({ error: "Unsupported app name" });
    }
    res.json({ user: updatedUser, message: "User updated" });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.delete("/identity", authMiddleware, async (req, res) => {
  const { userId, table = "users" } = req.body;
  const appName = req.headers["x-app-name"];
  if (!userId || !appName)
    return res.status(400).json({ error: "Missing userId or app name" });

  try {
    let result;
    if (appName === "rivas") {
      result = await require("../services/mongo").deleteUser(userId, table);
    } else if (appName === "yuga") {
      result = await require("../services/yuga").deleteUser(userId, table);
    } else if (appName === "vitess" || appName === "ecommerce") {
      result = await require("../services/vitess").deleteUser(userId, table);
    } else if (appName === "scylla") {
      result = await require("../services/scylla").deleteUser(userId, table);
    } else {
      return res.status(400).json({ error: "Unsupported app name" });
    }
    res.json({ message: "User deleted", result });
  } catch (err) {
    res.status(500).json({ error: err.message });
  }
});

router.get("/users", authMiddleware, async (req, res) => {
  const { page = 1, limit = 20, table = "users" } = req.query;
  const appName = req.headers["x-app-name"];
  const skip = (parseInt(page) - 1) * parseInt(limit);

  try {
    let users = [];
    if (appName === "rivas") {
      users = await require("../services/mongo").listUsers(table, skip, parseInt(limit));
    } else if (appName === "yuga") {
      users = await require("../services/yuga").listUsers(table, skip, parseInt(limit));
    } else if (appName === "vitess" || appName === "ecommerce") {
      users = await require("../services/vitess").listUsers(table, skip, parseInt(limit));
    } else if (appName === "scylla") {
      users = await require("../services/scylla").listUsers(table, skip, parseInt(limit));
    } else {
      return res.status(400).json({ error: "Unsupported app name" });
    }
    res.json({ users, page: parseInt(page), limit: parseInt(limit) });
  } catch (err) {
    res.status(500).json({ error: err.message });
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
