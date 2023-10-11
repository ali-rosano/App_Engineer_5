from config.mongo_database import collection

async def store_data(data):
    collection.insert_many(data)
    print('Data stored successfully 🚀')