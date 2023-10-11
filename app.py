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

# Configura la conexión a Redis (cambia la dirección y el puerto si es necesario)
redis_client = redis.Redis(host='localhost', port=6379, db=0)


class NetData(faust.Record):
    address: str
    IPv4: str


class PersonalData(faust.Record):
    name: str
    last_name: str
    sex: str
    telfnumber: str
    passport: str
    email: str


topic = app.topic('probando')


@app.agent(topic)
async def process_data(messages):
    async for message in messages:
        print(message)

        # Verifica la clase del mensaje y almacena en Redis en consecuencia
        if isinstance(message, NetData):
            # Almacena los datos de NetData en Redis
            redis_key = f'netdata_{message.address}'  # Clave única basada en la dirección
            redis_value = message.asdict()  # Convierte el mensaje a un diccionario
        elif isinstance(message, PersonalData):
            # Almacena los datos de PersonalData en Redis
            redis_key = f'personaldata_{message.name}'  # Clave única basada en el nombre
            redis_value = message.asdict()  # Convierte el mensaje a un diccionario
        else:
            print(f'Mensaje de tipo desconocido: {message}')
            continue  # Ignora mensajes de tipos desconocidos

        # Guarda el mensaje en Redis
        redis_client.set(redis_key, json.dumps(redis_value))
        print(f'Mensaje guardado en Redis: {redis_key}')


if __name__ == '__main__':
    app.main()
