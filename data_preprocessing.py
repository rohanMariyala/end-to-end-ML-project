import mysql.connector.cursor
import pandas as pd
import numpy as np
import mysql.connector
import getpass
from sqlalchemy import create_engine
import os

# READ DATA FROM MySQL TABLE #

password = getpass.getpass("Enter the password for the MySQL Database: \n")

db_path = mysql.connector.connect(
    host = "localhost",
    user = "root",
    password = password,
    database = "ml_project"
)

db_cur = db_path.cursor()
db_cur.execute("SELECT file_path from file_paths WHERE id = 1")

csv_path = db_cur.fetchone()[0]

# print(type(csv_path))

# LOAD THE DATA AS PANDAS DATAFRAME #

df = pd.read_csv(csv_path)
# csv_file.info()

# PREPROCESSING #

df = df.dropna()
df = df.join(pd.get_dummies(df['ocean_proximity'], dtype=int))
df = df.sample(n = len(df), random_state=1)
df = df.drop('ocean_proximity', axis = 1)
df = df[['longitude','latitude','housing_median_age',
         'total_rooms','total_bedrooms','population',
         'households','median_income','<1H OCEAN','INLAND',
         'ISLAND','NEAR BAY','NEAR OCEAN','median_house_value']]

# df.info()
# print(df.head())

# SAVE PREPROCESSED DATA AND SAVE IT'S PATH INTO MySQL TABLE #

pre_csv = 'preprocessed_data.csv'
file_name = "preprocessed_data"
df.to_csv(pre_csv, index = False) 
file_path = os.path.abspath(pre_csv)
print(file_path)
pre_save_query =  "INSERT INTO file_paths (file_name, file_path) VALUES (%s, %s) ON DUPLICATE KEY UPDATE file_name = VALUES(file_name), file_path = VALUES(file_path)"

# db_cur.execute(f"INSERT INTO file_paths VALUES (2, preprocessed data,{pre_csv_path})")

db_cur.execute(pre_save_query, (file_name, file_path))
db_path.commit()

db_path.close()
db_cur.close()









