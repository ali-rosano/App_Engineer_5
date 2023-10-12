from config.redis_server import redis_client

async def create_hash_for_passport(redis_client, key, data):
    redis_client.hset(key, mapping=data)
    if redis_client.hlen(key) > 5:
        hash_data = redis_client.hgetall(key)
        new_fullname = hash_data.get(b'fullname').decode('utf-8')
        new_fullname_hash = f'fullname: {new_fullname}'
        redis_client.hset(new_fullname_hash, mapping=hash_data)
        redis_client.delete(key)
        
async def create_hash_for_address(redis_client, key, data):
    redis_client.hset(key, mapping=data)
    if redis_client.hlen(key) > 3:
        hash_data = redis_client.hgetall(key)
        new_fullname = hash_data.get(b'fullname').decode('utf-8')
        new_fullname_hash = f'fullname: {new_fullname}'
        redis_client.hset(new_fullname_hash, mapping=hash_data)
        redis_client.delete(key)

async def create_hash_for_fullname(redis_client, key, data):
    redis_client.hset(key, mapping=data)