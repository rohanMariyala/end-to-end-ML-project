import pandas as pd
import numpy as np
import os
from db_connection import db_connector
from sklearn.model_selection import train_test_split
from sklearn.ensemble import RandomForestRegressor
import joblib
from sklearn.metrics import mean_absolute_error, mean_squared_error, root_mean_squared_error
from db_connection import SQL

# LOAD PREPROCESSED DATA FROM MySQL #

sql = SQL()
query = "SELECT file_path FROM file_paths WHERE id = 2"
pre_csv_path = sql.run_query_fetch(query, fetch_one=True)[0]
pre_csv = pd.read_csv(pre_csv_path)

# CREATE X & Y #

X = pre_csv.drop("median_house_value", axis=1)
y = pre_csv['median_house_value']

# Perform Train-Validaition Split #

X_train, X_test, y_train, y_test = train_test_split(X, y, test_size=0.2, random_state=42)
X_train_new, X_val, y_train_new, y_val = train_test_split(X_train, y_train, test_size=0.25, random_state=42)

# BUILD A ML MODEL #

forest = RandomForestRegressor()
forest.fit(X_train, y_train)

# COMPUTE ERROR METRICS #

def error_metrics(model, X_val, y_val):
    pred = model.predict(X_val)
    mae = mean_absolute_error(y_val, pred)
    mse = mean_squared_error(y_val, pred)
    rmse = root_mean_squared_error(y_val, pred)
    print("Mean absolute Error: \n", mae)
    print("Mean Square Error: \n",mse)
    print("Root Mean Square Error: \n",rmse)

    return mae, mse, rmse

error_metrics(forest, X_val, y_val)

# SAVE THE MODEL AS A JOB-LIB FILE & UPDATE THE PATH IN MySQL TABLE #

model_file_name = 'model_predictor_housing_price.joblib'
joblib.dump(forest, model_file_name)
file_path = os.path.abspath(model_file_name)
print(file_path)
model_save_query = """
INSERT INTO file_paths (file_name, file_path) 
VALUES (%s, %s) ON DUPLICATE KEY UPDATE 
file_name = VALUES(file_name), file_path = VALUES(file_path)
"""

sql.run_query(model_save_query, (model_file_name, file_path))

# SAVE THE TEST DATA & UPDATE THE PATH IN MySQL #

test_data = pd.concat([X_test, y_test], axis = 1)
test_file_name = 'test_data.csv'
test_data.to_csv(test_file_name, index = False)
file_path = os.path.abspath(test_file_name)
print(file_path)
model_save_query = """
INSERT INTO file_paths (file_name, file_path) 
VALUES (%s, %s) ON DUPLICATE KEY UPDATE 
file_name = VALUES(file_name), file_path = VALUES(file_path)
"""

sql.run_query(model_save_query, (test_file_name, file_path))