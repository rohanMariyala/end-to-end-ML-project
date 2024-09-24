import mysql.connector
import mysql.connector.cursor
from mysql.connector import Error

def db_connector():
    try:
        db_path = mysql.connector.connect(
            host = "172.16.1.174",
            user = "new_user",
            password = 'mysql',
            database = "ml_project"
        )
        print("MySQL Connection Succesful !\n")
        return db_path
    except Error as e:
        print(f"An error occured {e}")
    except Exception as e:
        print(f"An error occurred {e}")
        return None

class SQL:
    def __init__(self):
        self.db_path = db_connector()
        if self.db_path is not None:
            self.db_cur = self.db_path.cursor()
        else:
            raise Exception("Database connection failed")

    def run_query(self, query, params = None):
        try:
            if params:
                self.db_cur.execute(query, params)
            else:
                self.db_cur.execute(query)
            self.db_path.commit()
        except Exception as e:
            print(f"An error occurred during run_query: {e}")
            self.db_path.rollback()  # Rollback in case of error
            raise
        
    def run_query_fetch(self, query, fetch_one=False, params = None):
        if params:
            self.db_cur.execute(query, params)
        else:
            self.db_cur.execute(query)
        if fetch_one == True:
            result = self.db_cur.fetchone()
            self.db_cur.fetchall()
        else:
            result = self.db_cur.fetchall()
        return result
    
    def close(self):
        self.db_path.close()
        self.db_cur.close()