const { MongoClient } = require('mongodb');
require('dotenv').config();

let mongoCollection;

async function connectMongo() {
  const client = new MongoClient(process.env.MONGO_URI);
  await client.connect();
  mongoCollection = client.db().collection('users');
  console.log('✅ Connected to MongoDB');
}

function findUser(filter) {
  return mongoCollection.findOne(filter);
}

function upsertUser(user) {
  const { userId, ...rest } = user;
  return mongoCollection.updateOne({ userId }, { $set: rest }, { upsert: true });
}

module.exports = { connectMongo, findUser, upsertUser };