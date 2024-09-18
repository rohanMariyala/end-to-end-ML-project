import pandas as pd
import os
from db_connection import SQL
from datetime import datetime

# READ DATA FROM MySQL TABLE #
    
sql = SQL()
# query = "SELECT File_path FROM file_paths WHERE id = 1" ##
query = """
SELECT File_path FROM task_output_file_paths WHERE pipeline_run_id = '20240918-pre'
"""

error_message = None
status = 1

try:
    csv_path = sql.run_query_fetch(query, fetch_one=True)[0]
    print(csv_path)

    # LOAD THE DATA AS PANDAS DATAFRAME #

    df = pd.read_csv(csv_path)

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

except Exception as e:
    error_message = str(e)
    status = 0


now = datetime.now()
today = now.date()
print(today)
date_str = today.strftime('%Y%m%d')
print(date_str)
count = 0

count_query = """
SELECT COUNT(*) FROM task_output_file_paths
WHERE Task_name = 'pre' AND Task_type = 'output'
"""
count = (sql.run_query_fetch(count_query, fetch_one=True)[0]) + 1
print(count)

task_name = 'pre'
pipeline_run_id = f"{date_str}-{task_name}-{count:02d}"
print(pipeline_run_id)
pre_save_query = """
INSERT INTO task_output_file_paths (
	pipeline_run_id,
    Task_time,
    Task_name,
    Task_type,
    File_path,
    Error_message,
    Status
) VALUES (%s, %s, %s, %s, %s, %s, %s);
"""

query_data = (
    pipeline_run_id,
    date_str,
    task_name,
    'output',
    file_path,
    error_message,
    str(status)
)

sql.run_query(pre_save_query, query_data)
sql.close()
print("Success!")
