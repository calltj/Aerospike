const cassandra = require("cassandra-driver");
const logger = require("../logger");
let scyllaConn;

async function connectScylla() {
  const tempClient = new cassandra.Client({
    contactPoints: [
      process.env.SCYLLA_NODE0,
      process.env.SCYLLA_NODE1,
      process.env.SCYLLA_NODE2,
    ].filter(Boolean),
    localDataCenter: process.env.SCYLLA_LOCAL_DC || "AWS_US_EAST_1",
    credentials: {
      username: process.env.SCYLLA_USERNAME,
      password: process.env.SCYLLA_PASSWORD,
    },
  });
  try {
    await tempClient.connect();
    logger.info(" Connected to ScyllaDB cluster successfully!");

    await tempClient.execute(`
      CREATE KEYSPACE IF NOT EXISTS my_keyspace
      WITH replication = {'class': 'SimpleStrategy', 'replication_factor': '3'}
    `);
    logger.info(" Keyspace 'my_keyspace' created or already exists.");
    await tempClient.execute(`
  CREATE TABLE IF NOT EXISTS my_keyspace.users (
    userId text PRIMARY KEY,
    name text,
    email text,
    age int,
    balance double,
    lastSyncedAt timestamp
  )
`);
    await tempClient.execute(`
  CREATE INDEX IF NOT EXISTS users_email_idx ON my_keyspace.users (email)
`);
    logger.info(" Index on 'email' created or already exists.");
    logger.info(" Table 'users' created or already exists.");
  } catch (err) {
    logger.error("❌ ScyllaDB connection error:", err);
    throw err;
  } finally {
    await tempClient.shutdown();
  }

  scyllaConn = new cassandra.Client({
    contactPoints: [
      process.env.SCYLLA_NODE0,
      process.env.SCYLLA_NODE1,
      process.env.SCYLLA_NODE2,
    ].filter(Boolean),
    localDataCenter: process.env.SCYLLA_LOCAL_DC || "AWS_US_EAST_1",
    credentials: {
      username: process.env.SCYLLA_USERNAME,
      password: process.env.SCYLLA_PASSWORD,
    },
    keyspace: process.env.SCYLLA_KEYSPACE || "my_keyspace",
  });

  await scyllaConn.connect();
  logger.info(" Connected to ScyllaDB (with keyspace)!");
}

async function findScylla(query, table = "users") {
  let result = null;
  if (query.userId) {
    const res = await scyllaConn.execute(
      `SELECT * FROM ${table} WHERE userId = ? LIMIT 1`,
      [query.userId],
      { prepare: true }
    );
    result = res.rows[0];
  } else if (query.email) {
    const res = await scyllaConn.execute(
      `SELECT * FROM ${table} WHERE email = ? LIMIT 1`,
      [query.email],
      { prepare: true }
    );
    result = res.rows[0];
  }
  return result || null;
}

async function upsertScylla(user, table = "users") {
  await scyllaConn.execute(
    `INSERT INTO ${table} (userId, name, email, age, balance, lastSyncedAt)
     VALUES (?, ?, ?, ?, ?, ?)`,
    [
      user.userId,
      user.name,
      user.email,
      user.age,
      user.balance || 0,
      user.lastSyncedAt,
    ],
    { prepare: true }
  );
}

async function deleteUser(userId, table = "users") {
  await scyllaConn.execute(`DELETE FROM ${table} WHERE userId = ?`, [userId], {
    prepare: true,
  });
  return { userId };
}

async function listUsers(table = "users", skip = 0, limit = 20) {
  const res = await scyllaConn.execute(
    `SELECT * FROM ${table} LIMIT ? OFFSET ? ALLOW FILTERING`,
    [limit, skip],
    { prepare: true }
  );
  return res.rows;
}

module.exports = {
  connectScylla,
  findScylla,
  upsertScylla,
  deleteUser,
  listUsers,
};
