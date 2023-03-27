import { MongoClient } from "mongodb";

export class MongoAdapter {
  /**
   * @param {string} uri
   * @param {string} collection
   */
  constructor(uri, collection) {
    this.uri = uri;
    this.collection = collection;
    this.db = null;
    this.client = null;
    this.connect();
  }

  async connect() {
    this.client = await MongoClient.connect(this.uri);
    this.db = this.client.db();
  }

  /**
   * @param {string} name
   */
  getCollection(name) {
    if (!this.db) {
      throw new Error("Database not connected");
    }
    return this.db.collection(name);
  }

  async get(query) {
    return await this.getCollection(this.collection).findOne(query);
  }

  async put({ docName, version, value }) {
    if (!docName || !version || !value) {
      throw new Error("Missing required properties: docName, version, value");
    }
    return await this.getCollection(this.collection).updateOne(
      { docName, version },
      { $set: { value } },
      { upsert: true }
    );
  }

  async delete(query) {
    return await this.getCollection(this.collection).deleteMany(query);
  }

  async readAsCursor(query, { reverse = false, limit = null } = {}) {
    let cursor = this.getCollection(this.collection).find(query);
    if (reverse) {
      cursor = cursor.sort({ clock: -1 });
    }
    if (limit) {
      cursor = cursor.limit(limit);
    }
    return await cursor.toArray();
  }

  async close() {
    if (this.client) await this.client.close();
  }

  async flush() {
    if (this.db) {
      await this.db.dropDatabase();
      await this.close();
    }
  }
}
