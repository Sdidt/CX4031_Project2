import os
import psycopg2
from dotenv import load_dotenv

load_dotenv()

class DB:
    def __init__(self, host="127.0.0.1", database="TPC-H", user=os.getenv('DB_USERNAME'), password=os.getenv('DB_PASSWORD'), port=5432) -> None:
        self.conn = psycopg2.connect(database=database, user=user, password=password, host=host, port=port)
        self.cursor = self.conn.cursor()
        self.execute("SET statement_timeout = '180s'")
        print("Connection established")
        return
    
    def execute(self, query):
        self.cursor.execute(query)
        query_results = None
        try:
            query_results = self.cursor.fetchall()
        except psycopg2.ProgrammingError:
            print("Command executed: {} has no results".format(query))
        return query_results
    
    def close(self):
        self.cursor.close()
        self.conn.close()