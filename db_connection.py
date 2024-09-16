import mysql.connector
import mysql.connector.cursor
from mysql.connector import Error
import getpass

def db_connector():
    password = getpass.getpass("Enter the MySQL DB Password: \n")
    try:
        db_path = mysql.connector.connect(
            host = "localhost",
            user = "root",
            password = password,
            database = "ml_project"
        )
        return db_path
    except Error as e:
        print(f"An error occured {e}")
    except Exception as e:
        print(f"An error occurred {e}")
        return None

class SQL:
    def __init__(self):
        self.db_path= db_connector()
        self.db_cur = self.db_path.cursor()

    def run_query(self, query, params = None):
        if params:
            self.db_cur.execute(query, params)
        else:
            self.db_cur.execute(query)
        self.db_path.commit()
        return None
    def run_query_fetch(self, query, fetch_one=False, params = None):
        if params:
            self.db_cur.execute(query, params)
        else:
            self.db_cur.execute(query)
        if fetch_one == True:
            result = self.db_cur.fetchone()
        else:
            result = self.db_cur.fetchall()
        return result
    def close(self):
        self.db_path.close()
        self.db_cur.close()
