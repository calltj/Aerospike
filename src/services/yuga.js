const { Client } = require("pg");
require("dotenv").config();

const yugaConn = new Client({
  host: process.env.YUGA_HOST,
  port: parseInt(process.env.YUGA_PORT),
  user: process.env.YUGA_USER,
  password: process.env.YUGA_PASSWORD,
  database: process.env.YUGA_DB,
  ssl: { rejectUnauthorized: false },
});

async function connectYuga() {
  await yugaConn.connect();
  console.log("✅ Connected to YugabyteDB");
}

async function findUser(userId, email, table = "users") {
  const res = await yugaConn.query(
    `SELECT * FROM "${table}" WHERE userId = $1 OR email = $2`,
    [userId || "", email || ""]
  );
  return res.rows[0] || null;
}

async function upsertUser(user, table = "users") {
  await yugaConn.query(
    `INSERT INTO "${table}" (userId,name,email,age,balance,lastSyncedAt)
     VALUES ($1,$2,$3,$4,$5,$6)
     ON CONFLICT (userId) DO UPDATE
     SET name=$2, age=$4, balance=$5, lastSyncedAt=$6`,
    [
      user.userId,
      user.name,
      user.email,
      user.age,
      user.balance || 0,
      user.lastSyncedAt,
    ]
  );
}
module.exports = { connectYuga, findUser, upsertUser };
