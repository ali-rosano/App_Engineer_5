import faust
import threading
from controllers.startStreaming import start_streaming
from config.mongo_database import collection
from controllers.createStructSQL import create_mysql_table
from controllers.watchLonelyData import watch_lonely_data
from config.connection import KAFKA_HOST, KAFKA_PORT

app = faust.App(
    'my-kafka-consumer',
    broker=f'kafka://{KAFKA_HOST}:{KAFKA_PORT}',
    value_serializer='json',
)

probando_topic = app.topic('probando')
create_mysql_table()

if __name__ == '__main__':
    redis_thread = threading.Thread(target=watch_lonely_data)
    redis_thread.start()
    app.agent(probando_topic)(start_streaming)
    app.main()
