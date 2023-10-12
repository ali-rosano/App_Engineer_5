from config.mongo_database import collection

async def store_data_in_mongo(data):
    collection.insert_many(data)
    print('Data stored successfully in Mongo and SQL 🚀')