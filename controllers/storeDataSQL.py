from config.mysql_database import mysql_conn

def store_data_in_mysql(data_list):
    if not mysql_conn.is_connected():
        mysql_conn.ping(reconnect=True)
        
    cursor = mysql_conn.cursor()
    insert_query = """
        INSERT INTO users_data (
            IBAN, IPv4, address, city, company, company_address, company_email, company_telfnumber, email, fullname, job, passport, salary, sex, telfnumber
        ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        
    values = [
        (
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
        ) for data in data_list
    ]

    cursor.executemany(insert_query, values)
    mysql_conn.commit()
    cursor.close()