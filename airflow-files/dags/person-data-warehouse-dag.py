from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os
import sys

default_args = {
    'owner' : 'abhishekyadav',
    'start_date' : datetime(2023,11,1),
    'retries' : 1,
}

dag = DAG(
    'person-data-warehouse',
    default_args = default_args,
    schedule_interval=None,  # How often the DAG should run (e.g., '0 0 * * *' for daily)
    start_date=None,  # The initial date and time when the DAG should start
    catchup=False
)


# Making airflow as root directory 
sys.path.insert(0, '/home/abhishek9yadav07/airflow')

from scripts.p01_department import department_etl
from scripts.p01_survey import survey_etl
from scripts.p02_employee_pesonal_data import employee_personal_data_etl
from scripts.p03_employee_data import employee_data_etl

task1 =  PythonOperator (
    task_id='department_etl',
    python_callable=department_etl,
    dag=dag,
)

task2 = PythonOperator(
    task_id = 'survey_etl',
    python_callable = survey_etl,
    dag=dag,
)

task3 = PythonOperator(
    dag_id = 'employee_personal_data_etl',
    python_callable = employee_personal_data_etl,
    dag=dag,
)

task4 = PythonOperator(
    task_id = "employee_data_etl",
    python_callable = employee_data_etl,
    dag=dag,
)

# Set up task dependencies
task1 >> task3
task2 >> task3
task3 >> task4