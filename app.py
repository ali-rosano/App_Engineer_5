import faust
import os
import json
from pymongo import MongoClient
import dotenv

dotenv.load_dotenv()

MONGO_DB_URI = os.getenv('MONGO_DB_URI')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')

client = MongoClient(MONGO_DB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]

app = faust.App(
    'my-kafka-consumer',
    broker='kafka://localhost:29092',
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