from config.redis_server import redis_client
from controllers.storeData import store_data

async def sort_and_send_data(data_list, pack_data):
    try:
        if 'fullname' in data_list:
            fullname = data_list['fullname']
            redis_key = f'fullname: {fullname}'
            
            if redis_client.hlen(redis_key) == 15:
                hash_data = redis_client.hgetall(redis_key)
                merged_data = {key.decode('utf-8'): value.decode('utf-8') for key, value in hash_data.items()}
                pack_data.append(dict(sorted(merged_data.items())))
                redis_client.delete(redis_key)

                if len(pack_data) == 5:
                    await store_data(pack_data)
                    pack_data.clear()
    except Exception as e:
        print("No se ha podido enviar a mongoDb", {e})