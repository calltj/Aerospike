const cassandra = require("cassandra-driver");
let scyllaConn;

async function connectScylla() {
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
    // sslOptions: {rejectUnauthorized: false},
    keyspace: process.env.SCYLLA_KEYSPACE || "my_keyspace",
  });
  await scyllaConn.connect();
  console.log("✅ Connected to ScyllaDB");
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

module.exports = { connectScylla, findScylla, upsertScylla };