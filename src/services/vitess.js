const mysql = require("mysql2/promise");
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
  console.log("✅ Connected to Vitess");
}

async function upsertVitess(user) {
  const sql = `
    INSERT INTO users (userId, name, email, age, balance)
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

async function findVitess(query) {
  if (query.userId) {
    const [rows] = await vitessConn.query(
      "SELECT * FROM users WHERE userId = ?",
      [query.userId]
    );
    return rows[0];
  } else if (query.email) {
    const [rows] = await vitessConn.query(
      "SELECT * FROM users WHERE email = ?",
      [query.email]
    );
    return rows[0];
  }
  return null;
}

module.exports = { connectVitess, upsertVitess, findVitess };
