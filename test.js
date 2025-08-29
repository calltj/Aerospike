const axios = require("axios");

const API_URL = "https://aerospike.brivas.io/api/identity"; // <-- login endpoint
const loginData = {
  email: "mongo.user@example.com",
  // password: 'yourPassword' // Uncomment if your API requires a password
};

async function login() {
  try {
    const res = await axios.post(API_URL, loginData, {
      headers: { "x-app-name": "mongodb" },
    });
    logger.info("[✅ MONGODB LOGIN] Response:", res.data);
  } catch (err) {
    logger.error(
      "[❌ MONGODB LOGIN] Error:",
      err.response?.data || err.message
    );
  }
}

login();
