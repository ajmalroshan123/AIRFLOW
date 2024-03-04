from airflow import DAG
from airfow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import random

def generate_random_number():
    return random.randint(1,100)

def print_random_number(**kwargs):
    task_instance = kwargs['ti']
    random_number = task_instance.xcom_pull(task_id = generate_random_number_task)
    print(f"Random number generated: {random_number}")

defult_args = {
    'owner':'airflow'
    'depends_on_past':False,
    "start_date":datetime(2024, 3, 3)
    'reties':1,
    'retry_delay':timedelta(minutes = 5)

}    

with DAG ('sheduled_task_dag',
          defult_args = defult_args,
          description = "Scheduled Airflow DAG with Python operator",
          shedule_interval = timedelta(minutes = 5),
          catchup = False
          ) as dag:
    
    generate_random_number_task = PythonOperator(
        task_id = 'generate_random_number_task',
        python_callable = print_random_number
    )

    print_random_number_task = PythonOperator(
        task_id = 'print_random_number_task',
        python_callable = print_random_number
    )

    generate_random_number_task >> print_random_number_task
