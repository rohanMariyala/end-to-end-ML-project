import pandas as pd
import os
from db_connection import SQL
from datetime import datetime

# READ DATA FROM MySQL TABLE #
    
sql = SQL()
csv_path_query = "SELECT File_path FROM task_output_file_paths WHERE pipeline_run_id = '20240919-pre-00'"
error_message = None
status = 1
now = datetime.now()
today = now.date()
date_str = today.strftime('%Y%m%d')
task_name = 'pre-processing'
count = 0
count_query = "SELECT COUNT(*) FROM task_output_file_paths WHERE Task_name = 'pre-processing' AND Task_type = 'output'"
count = (sql.run_query_fetch(count_query, fetch_one=True)[0]) + 1
pipeline_run_id = f"{date_str}-{task_name[0:3]}-{count:02d}"

try:
    csv_path = sql.run_query_fetch(csv_path_query, fetch_one=True)[0]

    if not csv_path:
        raise ValueError("csv_path not available")

    # LOAD THE DATA AS PANDAS DATAFRAME #

    df = pd.read_csv(csv_path)

    # PREPROCESSING #

    df = df.dropna() 
    df = df.join(pd.get_dummies(df['ocean_proximity'], dtype=int)) 
    df = df.drop('ocean_proximity', axis = 1)

    # SAVE PREPROCESSED DATA AND SAVE IT'S PATH INTO MySQL TABLE #

    output_dir = sql.config_data["output_dir"]

    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pre_csv = os.path.join(output_dir, 'preprocessed_data.csv')
    df.to_csv(pre_csv, index=False)
    file_path = os.path.abspath(pre_csv)
    
    if not file_path:
        raise ValueError("file_path not available")
    
except Exception as e:
    error_message = str(e)
    print(f"An error occurred: {error_message}")
    status = 0
    sql.sql_return_error(pipeline_run_id, date_str, task_name, error_message,status)
    raise
    
output_type = 'output'
print("Pipeline run ID: \n",pipeline_run_id)

sql.save_model_query(pipeline_run_id, date_str, task_name, output_type, file_path, error_message, status)

sql.close()
print("Preprocessing Successful !")