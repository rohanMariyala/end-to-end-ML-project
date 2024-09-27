import pandas as pd
import os
from db_connection import SQL
from datetime import datetime

# READ DATA FROM MySQL TABLE #
    
sql = SQL()
query = """
SELECT File_path FROM task_output_file_paths WHERE pipeline_run_id = '20240919-pre-00'
"""
error_message = None
status = 1

try:
    csv_path = sql.run_query_fetch(query, fetch_one=True)[0]

    # LOAD THE DATA AS PANDAS DATAFRAME #

    df = pd.read_csv(csv_path)

    # PREPROCESSING #

    df = df.dropna() # dropping null values
    df = df.join(pd.get_dummies(df['ocean_proximity'], dtype=int)) #one-hot encoding
    df = df.drop('ocean_proximity', axis = 1)

    # SAVE PREPROCESSED DATA AND SAVE IT'S PATH INTO MySQL TABLE #

    output_dir = '//mnt//c//users//RohanMariyala//MyPracticeTrack//end-to-end-ml-files'
    if not os.path.exists(output_dir):
        os.makedirs(output_dir)

    pre_csv = os.path.join(output_dir, 'preprocessed_data.csv')
    df.to_csv(pre_csv, index=False)

    file_path = os.path.abspath(pre_csv)

except Exception as e:
    error_message = str(e)
    print(e)
    status = 0


now = datetime.now()
today = now.date()
date_str = today.strftime('%Y%m%d')

count = 0
count_query = """
SELECT COUNT(*) FROM task_output_file_paths
WHERE Task_name = 'pre-processing' AND Task_type = 'output'
"""
count = (sql.run_query_fetch(count_query, fetch_one=True)[0]) + 1
task_name = 'pre-processing'
output_type = 'output'
pipeline_run_id = f"{date_str}-{task_name[0:3]}-{count:02d}"
print("Pipeline run ID: \n",pipeline_run_id)

sql.save_model_query(pipeline_run_id, date_str, task_name, output_type, file_path, error_message, status)

sql.close()
print("Preprocessing Successful !")