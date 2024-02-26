from airflow import DAG
from ariflow.opertors.python_opertor import PythonOperator
from datetime import datetime

def my_first_task():
  print("Hello, Airflow")

with DAG(
  "my_first_task",
  description = "My First Airflow DAG",
  schedule_interval = None,
  start_date = datetime(2024,2,25)
) as dag

first_task = pythonOperator(
  task_id = 'first_task',
  python_callable = my_first_task
)
