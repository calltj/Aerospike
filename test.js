const axios = require("axios");

const API_URL = "https://aerospike.brivas.io/api/identity"; // <-- signup endpoint
const signupData = {
  user: {
    userId: "mongo123",
    name: "Mongo User",
    email: "mongo.user@example.com",
    password: "yourPassword", // Replace with the actual password
    age: 28,
    balance: 2000,
  },
  table: "users",
};

async function signup() {
  try {
    const res = await axios.post(API_URL, signupData, {
      headers: { "x-app-name": "vitess" },
    });
    console.log("[ VITESS SIGNUP] Response:", res.data);
  } catch (err) {
    console.error(
      "[❌ VITESS SIGNUP] Error:",
      err.response?.data || err.message
    );
  }
}

signup();
