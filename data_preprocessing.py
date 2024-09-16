import pandas as pd
import os
from db_connection import SQL

# READ DATA FROM MySQL TABLE #
    
sql = SQL()
query = "SELECT file_path FROM file_paths WHERE id = 1"
csv_path = sql.run_query_fetch(query, fetch_one=True)[0]

# LOAD THE DATA AS PANDAS DATAFRAME #

df = pd.read_csv(csv_path)
df.info()

# PREPROCESSING #

df = df.dropna() # dropping null values
df = df.join(pd.get_dummies(df['ocean_proximity'], dtype=int)) #one-hot encoding
df = df.drop('ocean_proximity', axis = 1)

# SAVE PREPROCESSED DATA AND SAVE IT'S PATH INTO MySQL TABLE #

pre_csv = 'preprocessed_data.csv'
df.to_csv(pre_csv, index = False)
file_name = "preprocessed_data" 
file_path = os.path.abspath(pre_csv)
print(file_path)
pre_save_query = """
INSERT INTO file_paths (file_name, file_path) 
VALUES (%s, %s) ON DUPLICATE KEY UPDATE 
file_name = VALUES(file_name), file_path = VALUES(file_path)
"""

sql.run_query(pre_save_query, (file_name, file_path))
sql.close()
print("Success!")









