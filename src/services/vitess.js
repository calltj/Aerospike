const mysql = require("mysql2/promise");
const logger = require("./logger");
require("dotenv").config();

const vitessConn = mysql.createPool({
  host: process.env.VITESS_HOST,
  port: process.env.VITESS_PORT,
  user: process.env.VITESS_USER,
  password: process.env.VITESS_PASSWORD,
  database: process.env.VITESS_DB,
  waitForConnections: true,
  connectionLimit: 10,
  queueLimit: 0,
});

async function connectVitess() {
  // Test connection
  await vitessConn.getConnection();
  logger.info("✅ Connected to Vitess");
}

async function findVitess(query, table = "users") {
  if (query.userId) {
    const [rows] = await vitessConn.query(
      `SELECT * FROM \`${table}\` WHERE userId = ?`,
      [query.userId]
    );
    return rows[0];
  } else if (query.email) {
    const [rows] = await vitessConn.query(
      `SELECT * FROM \`${table}\` WHERE email = ?`,
      [query.email]
    );
    return rows[0];
  }
  return null;
}

async function upsertVitess(user, table = "users") {
  const sql = `
    INSERT INTO \`${table}\` (userId, name, email, age, balance)
    VALUES (?, ?, ?, ?, ?)
    ON DUPLICATE KEY UPDATE name=VALUES(name), email=VALUES(email), age=VALUES(age), balance=VALUES(balance)
  `;
  await vitessConn.query(sql, [
    user.userId,
    user.name,
    user.email,
    user.age,
    user.balance,
  ]);
}

async function deleteUser(userId, table = "users") {
  await vitessConn.query(
    `DELETE FROM \`${table}\` WHERE userId = ?`,
    [userId]
  );
  return { userId };
}

async function listUsers(table = "users", skip = 0, limit = 20) {
  const [rows] = await vitessConn.query(
    `SELECT * FROM \`${table}\` LIMIT ? OFFSET ?`,
    [limit, skip]
  );
  return rows;
}


module.exports = { connectVitess, upsertVitess, findVitess, deleteUser, listUsers};
