from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.bash_operator import BashOperator
from datetime import datetime

def print_message():
   print("This is the python function executing in the airflow")

with DAG('advanced_task_dag',
         description = 'Advanced Airflow dag with PythonOperator and BashOperator',
         schedule_interval = '@daily',
         start_date = datetime(2024,2,28),
         catchup = False) as dag:
           
    python_task = PythonOperator(
      task_id = 'python_task',
      python_callable  = print_message,
      dag = dag
    )

    bash_task = BashOperator(
      task_id = 'bash_task',
      bash_command = 'echo"This is the Bash command executing in Airflow"',
      dag = dag
    )

    python_task >> bash_task       

           
