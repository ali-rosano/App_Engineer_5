import faust
import os
import json
import redis
from pymongo import MongoClient
import dotenv
import redis
import mysql.connector
from create_table import create_mysql_table

dotenv.load_dotenv()

MONGO_DB_URI = os.getenv('MONGO_DB_URI')
MONGODB_DATABASE = os.getenv('MONGODB_DATABASE')
MONGODB_COLLECTION = os.getenv('MONGODB_COLLECTION')
REDIS_HOST = os.getenv('REDIS_HOST')
REDIS_PORT = os.getenv('REDIS_PORT')
REDIS_PASSWORD = os.getenv('REDIS_PASSWORD')
KAFKA_HOST = os.getenv('KAFKA_HOST')
KAFKA_PORT = os.getenv('KAFKA_PORT')
MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

create_mysql_table()
client = MongoClient(MONGO_DB_URI)
db = client[MONGODB_DATABASE]
collection = db[MONGODB_COLLECTION]

redis_client = redis.Redis(
    host=REDIS_HOST,
    port=REDIS_PORT,
    password=REDIS_PASSWORD)

mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)

app = faust.App(
    'my-kafka-consumer',
    broker=f'kafka://{KAFKA_HOST}:{KAFKA_PORT}',
    value_serializer='json',
)


async def store_data_in_mysql(data):
    try:
        cursor = mysql_conn.cursor()

        insert_query = """
        INSERT INTO users_data (
            IBAN, IPv4, address, city, company, company_address, company_email, company_telfnumber, email, fullname, job, passport, salary, sex, telfnumber
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
        values = (
            data.get('IBAN'),
            data.get('IPv4'),
            data.get('address'),
            data.get('city'),
            data.get('company'),
            data.get('company address'),
            data.get('company_email'),
            data.get('company_telfnumber'),
            data.get('email'),
            data.get('fullname'),
            data.get('job'),
            data.get('passport'),
            data.get('salary'),
            data.get('sex'),
            data.get('telfnumber')
        )

        cursor.execute(insert_query, values)
        mysql_conn.commit()
        cursor.close()

        print("Datos almacenados en MySQL.")
    except mysql.connector.Error as err:
        print(f"Error al guardar en MySQL: {err}")


async def store_data(data):
    collection.insert_one(data)

async def sort_and_send_data(data_list):
    try:
        if fullname := data_list.get('fullname'):
            redis_client.hset(f'fullname: {fullname}', mapping=data_list)
            if redis_client.exists(f'fullname: {fullname}') and redis_client.hlen(f'fullname: {fullname}') == 15:
                hash_data = redis_client.hgetall(f'fullname: {fullname}')
                merged_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in hash_data.items()}
                
                sorted_data = dict(sorted(merged_data.items()))

                await store_data(sorted_data)
                await store_data_in_mysql(sorted_data)
    except Exception:
        print("No se ha podido enviar a mongoDb")

       


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

            await sort_and_send_data(data)
            store_data_in_mysql(data)

        except json.JSONDecodeError as e:
            print(f"Error al decodificar JSON: {e}, Data: {data}")
    
    mysql_conn.close()


if __name__ == '__main__':
    app.main()

