import faust
import os
import json
import redis
from pymongo import MongoClient
import dotenv
import redis

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


probando_topic = app.topic('probando')


@app.agent(probando_topic)
async def process_probando(probando_stream):
    counter = 0
    counter_insertions = 0
    async for data in probando_stream:
        # print(f"Received Data: {data}")
        counter += 1
        print(f"Counter: {counter}")
        try:
            passport = data.get('passport')
            fullname = data.get('fullname')
            address = data.get('address')
            name = data.get('name')
            lastname = data.get('last_name')
            sex = data.get('sex')

            if passport:
                if name:
                    data['sex'] = sex[0] if sex else 'Not specified'
                    data['fullname'] = f"{name} {lastname}"
                    del data['name']
                    del data['last_name']
                redis_client.hset(f'passport: {passport}', mapping=data)
                if redis_client.hlen(f'passport: {passport}') > 5:
                    hash_data = redis_client.hgetall(f'passport: {passport}')
                    new_fullname = hash_data.get(b'fullname').decode('utf-8')
                    new_fullname_hash = f'fullname: {new_fullname}'
                    redis_client.hset(new_fullname_hash, mapping=hash_data)
                    redis_client.delete(f'passport: {passport}')
            elif address:
                redis_client.hset(f'address: {address}', mapping=data)
                if redis_client.hlen(f'address: {address}') > 3:
                    hash_data = redis_client.hgetall(f'address: {address}')
                    new_fullname = hash_data.get(b'fullname').decode('utf-8')
                    new_fullname_hash = f'fullname: {new_fullname}'
                    redis_client.hset(new_fullname_hash, mapping=hash_data)
                    redis_client.delete(f'address: {address}')
            elif fullname:
                redis_client.hset(f'fullname: {fullname}', mapping=data)



        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}, Data: {data}")


if __name__ == '__main__':
    app.main()
