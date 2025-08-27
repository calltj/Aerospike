const axios = require('axios');

const API_URL = 'https://aerospike.brivas.io/api/identity';
const testUser = {
  userId: 'vitess123',
  name: 'Vitess User',
  email: 'vitess.user@example.com',
  age: 32,
  balance: 1500,
};

async function signup() {
  try {
    const res = await axios.post(
      API_URL,
      { user: testUser },
      { headers: { 'x-app-name': 'vitess' } } // <-- Use 'vitess' here
    );
    console.log('[✅ SIGNUP] Response:', res.data);
  } catch (err) {
    console.error('[❌ SIGNUP] Error:', err.response?.data || err.message);
  }
}

signup();