const { MongoClient } = require("mongodb");
require("dotenv").config();
const logger = require("./logger");
let mongoCollection;

async function connectMongo() {
  const client = new MongoClient(process.env.MONGO_URI);
  await client.connect();
  mongoCollection = client.db().collection("users");
  logger.info("✅ Connected to MongoDB");
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

async function deleteUser(userId, collectionName = "users") {
  return mongoCollection
    .db()
    .collection(collectionName)
    .deleteOne({ userId });
}

async function listUsers(collectionName = "users", skip = 0, limit = 20) {
  return mongoCollection
    .db()
    .collection(collectionName)
    .find({})
    .skip(skip)
    .limit(limit)
    .toArray();
}

module.exports = { connectMongo, findUser, upsertUser,deleteUser, listUsers };
