from config.connection import MONGO_DB_URI, MONGODB_DATABASE, MONGODB_COLLECTION
from pymongo import MongoClient

client = MongoClient(MONGO_DB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]