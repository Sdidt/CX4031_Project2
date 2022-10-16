import os
from dotenv import load_dotenv

load_dotenv()
import psycopg2 

conn = psycopg2.connect(database="TPC-H", user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), host="127.0.0.1", port=5432)

cursor = conn.cursor()

cursor.execute("select version()")

data = cursor.fetchone()
print("Connection established to: ",data)

sql_query = 'SELECT * FROM customer C, orders O where C.c_custkey = O.o_custkey'

cursor.execute('explain analyze ' + sql_query)
analyze_fetched = cursor.fetchall()
print(analyze_fetched)

conn.close()
