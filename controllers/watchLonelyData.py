from config.redis_server import redis_client
from config.mongo_database import collection
from controllers.storeDataSQL import store_data_in_mysql
import time

def watch_lonely_data():
    while True:
        try:
            keys = redis_client.keys('*')
            for key in keys:
                if redis_client.hlen(key) >= 15:
                    hash_data = redis_client.hgetall(key)
                    merged_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in hash_data.items()}
                    pack_data = dict(sorted(merged_data.items()))
                    redis_client.delete(key)
                    
                    collection.insert_one(pack_data)
                    store_data_in_mysql([pack_data])
                    print("💥 Insert one element in MongoDb and MySQL 💥")

        except Exception as e:
            print(f"Error al procesar datos en Redis: {e}")
        time.sleep(3)