const axios = require('axios');
const { Client } = require('pg');
require('dotenv').config();

const API_URL = 'https://aerospike.brivas.io';
const APP_NAME = 'yuga'; // Targeting YugabyteDB

const testUser = {
  userId: 'yuga123',
  name: 'Timileyin',
  email: 'timileyin@yuga.com',
  age: 28,
  balance: 100,
};

async function signup() {
  try {
    const res = await axios.post(`${API_URL}/identity`, { user: testUser }, {
      headers: { 'x-app-name': APP_NAME }
    });
    console.log('[✅ SIGNUP] Response:', res.data);
  } catch (err) {
    console.error('[❌ SIGNUP] Error:', err.response?.data || err.message);
  }
}

async function login() {
  try {
    const res = await axios.post(`${API_URL}/auth`, { email: testUser.email }, {
      headers: { 'x-app-name': APP_NAME }
    });
    console.log('[🔐 LOGIN] Response:', res.data);
  } catch (err) {
    console.error('[❌ LOGIN] Error:', err.response?.data || err.message);
  }
}

async function checkYugabyte() {
  const client = new Client({
    host: process.env.YUGA_HOST,
    port: process.env.YUGA_PORT,
    user: process.env.YUGA_USER,
    password: process.env.YUGA_PASSWORD,
    database: process.env.YUGA_DB,
    ssl: {
      rejectUnauthorized: true,
      ca: require('fs').readFileSync('./certs/root.crt').toString(),
    },
  });

  try {
    await client.connect();
    const res = await client.query(
      'SELECT * FROM users WHERE email = $1',
      [testUser.email]
    );
    console.log('[🧠 YUGABYTE] DB Record:', res.rows[0] || 'Not found');
  } catch (err) {
    console.error('[❌ YUGABYTE] Error:', err.message);
  } finally {
    await client.end();
  }
}

(async () => {
  await signup();
  await login();
  await checkYugabyte();
})();