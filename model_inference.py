from db_connection import SQL
import pandas as pd
from flask import Flask, request, jsonify
import joblib
from sklearn.metrics import r2_score
import os
from datetime import datetime

sql = SQL()
error_message = None
status = 1

# LOAD THE TEST DATA FROM MySQL #

# query = "SELECT file_path FROM file_paths WHERE id = 4" ##

model_output_count_query =  """
SELECT COUNT(*) FROM task_output_file_paths
WHERE Task_name = 'training-model' AND Task_type = 'model'
"""
count = sql.run_query_fetch(model_output_count_query, fetch_one=True)[0] 
print(count)

test_data_query = """
SELECT File_path FROM task_output_file_paths 
WHERE Task_name = 'training-model' AND Task_type = 'test data'
"""
test_data_path = sql.run_query_fetch(test_data_query, fetch_one=True)[0]
print(test_data_path)
test_data = pd.read_csv(test_data_path)

# LOAD THE TRAINED ML MODEL FROM MySQL #

model_joblib_query = """
SELECT File_path FROM task_output_file_paths 
WHERE Task_name = 'training-model' AND Task_type = 'model'
"""
model_path = sql.run_query_fetch(model_joblib_query, fetch_one=True)[0]
print(model_path)
model = joblib.load(model_path)

X_test = test_data.drop(['median_house_value'], axis=1)
y_test = test_data['median_house_value']

# GIVE MODEL PREDICTION SCORE TO THE INPUT TEST VECTOR #

y_pred = model.predict(X_test)
r2 = r2_score(y_test, y_pred)
print("\nModel Prediction Score: ", r2)

# SAVE THE TEST DATA WITH THE PREEDICTIONS #
X_test['predictions'] = y_pred
pred_file_name = 'test_with_pred.csv'
X_test.to_csv(pred_file_name, index=False)

file_path = os.path.abspath(pred_file_name)

now = datetime.now()
today = now.date()
date_str = today.strftime('%Y%m%d')

count = 0
count_query = """
SELECT COUNT(*) FROM task_output_file_paths
WHERE Task_name = 'inference-model' AND Task_type = 'output'
"""
count = (sql.run_query_fetch(count_query, fetch_one=True)[0]) + 1
task_name = 'inference-model'
pipeline_run_id = f"{date_str}-{task_name[0:5]}-{count:02d}"
print("Pipeline run ID: \n",pipeline_run_id)
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
    'predictions',
    file_path,
    error_message,
    str(status)
)
try:
    sql.run_query(pre_save_query, query_data)
except Exception as e:
    print(e)

sql.close()