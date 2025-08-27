const { MongoClient } = require("mongodb");
require("dotenv").config();

let mongoCollection;

async function connectMongo() {
  const client = new MongoClient(process.env.MONGO_URI);
  await client.connect();
  mongoCollection = client.db().collection("users");
  console.log("✅ Connected to MongoDB");
}

async function findUser(filter, collectionName = "users") {
  return mongoCollection.db().collection(collectionName).findOne(filter);
}

async function upsertUser(user, collectionName = "users") {
  const { userId, ...rest } = user;
  return mongoCollection
    .db()
    .collection(collectionName)
    .updateOne({ userId }, { $set: rest }, { upsert: true });
}

module.exports = { connectMongo, findUser, upsertUser };
