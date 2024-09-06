import mysql.connector.cursor
import pandas as pd
import numpy as np
import mysql.connector
import getpass
import os
from db_connection import db_connector

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
    
# READ DATA FROM MySQL TABLE #
    
sql = SQL()
query = "SELECT file_path FROM file_paths WHERE id = 1"
csv_path = sql.run_query_fetch(query, fetch_one=True)[0]

# LOAD THE DATA AS PANDAS DATAFRAME #

df = pd.read_csv(csv_path)
df.info()

# PREPROCESSING #

df = df.dropna()
df = df.join(pd.get_dummies(df['ocean_proximity'], dtype=int))
# df = df.sample(n = len(df), random_state=1)
df = df.drop('ocean_proximity', axis = 1)

# SAVE PREPROCESSED DATA AND SAVE IT'S PATH INTO MySQL TABLE #

pre_csv = 'preprocessed_data.csv'
df.to_csv(pre_csv, index = False)
file_name = "preprocessed_data" 
file_path = os.path.abspath(pre_csv)
print(file_path)
pre_save_query = "INSERT INTO file_paths (file_name, file_path) VALUES (%s, %s) ON DUPLICATE KEY UPDATE file_name = VALUES(file_name), file_path = VALUES(file_path)"

sql.run_query(pre_save_query, (file_name, file_path))
sql.close()









