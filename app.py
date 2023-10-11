import faust
import os
import json
import redis
from pymongo import MongoClient
import dotenv

dotenv.load_dotenv()

MONGO_DB_URI = os.getenv('MONGO_DB_URI')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAFKA_PORT = os.getenv('KAFKA_PORT')

client = MongoClient(MONGO_DB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD)

app = faust.App(
    'my-kafka-consumer',
    broker=f'kafka://{KAFKA_HOST}:{KAFKA_PORT}',
    value_serializer='json',
)

class NetData(faust.Record):
    address: str
    IPv4: str
    
topic = app.topic('probando', value_type=NetData)

@app.agent(topic)
async def process_data(messages):
    async for message in messages:
        print(message)

if __name__ == '__main__':
    app.main()