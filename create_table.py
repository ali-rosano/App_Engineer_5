import mysql.connector
import dotenv
import os

dotenv.load_dotenv()


MYSQL_HOST = os.getenv('MYSQL_HOST')
MYSQL_USER = os.getenv('MYSQL_USER')
MYSQL_PASSWORD = os.getenv('MYSQL_PASSWORD')
MYSQL_DATABASE = os.getenv('MYSQL_DATABASE')

mysql_conn = mysql.connector.connect(
    host=MYSQL_HOST,
    user=MYSQL_USER,
    password=MYSQL_PASSWORD,
    database=MYSQL_DATABASE
)

def create_mysql_table():

    mysql_cursor = mysql_conn.cursor()

    create_table_query = """
    CREATE TABLE IF NOT EXISTS users_data (
        IBAN VARCHAR(30),
        IPv4 VARCHAR(20),
        address VARCHAR(255),
        city VARCHAR(255),
        company VARCHAR(255),
        company_address VARCHAR(255),
        company_email VARCHAR(255),
        company_telfnumber VARCHAR(30),
        email VARCHAR(255),
        fullname VARCHAR(255),
        job VARCHAR(255),
        passport VARCHAR(30),
        salary VARCHAR (50),
        sex VARCHAR(10),
        telfnumber VARCHAR(255)

    )
    """

    mysql_cursor.execute(create_table_query)

    mysql_conn.commit()

    mysql_cursor.close()
    mysql_conn.close()
