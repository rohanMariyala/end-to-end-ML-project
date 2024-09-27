from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime

# Define the default arguments for the DAG
default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 9, 20),  
    'retries': 1
}

# Define the DAG
with DAG(
    'end_to_end_ml',  # Name of the DAG
    default_args=default_args,
    schedule_interval=None,  
    catchup=False
) as dag:
    
    # Task 1: Run task1.py using BashOperator
    run_task1 = BashOperator(
        task_id='pre-processing',
        bash_command='python3 //mnt//c//Users//RohanMariyala//MyPracticeTrack//end-to-end-ML-project//data_preprocessing.py', 
    )

    # Task 2: Run task2.py using BashOperator
    run_task2 = BashOperator(
        task_id='model-training',
        bash_command='python3 //mnt//c//Users//RohanMariyala//MyPracticeTrack//end-to-end-ML-project//model_training.py',  
    )

    # Task 3: Run task3.py using BashOperator
    run_task3 = BashOperator(
        task_id='model-inference',
        bash_command='python3 //mnt//c//Users//RohanMariyala//MyPracticeTrack//end-to-end-ML-project//model_inference.py',  
    )

    # Set the task dependencies
    run_task1 >> run_task2 >> run_task3
