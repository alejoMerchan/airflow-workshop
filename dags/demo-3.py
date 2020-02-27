import logging
import os

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("Hello World")

def current_time():
    logging.info(f"Current time is {datetime.now()}")

def working_dir():
    logging.info("Working directory is {os.getcwd()}")

def complete():
    logging.info("Congrats, your first multi-task pipeline is now complete")

dag = DAG(
    "demo3",
    start_date=datetime(2020,2,20))

hello_world_task = PythonOperator(
    task_id="hello_world",
    python_callable=hello_world,
    dag=dag)


# Create Tasks and Operators

current_time_task = PythonOperator(
    task_id="current_time",
    python_callable=current_time,
    dag=dag
)

working_dir_task = PythonOperator(
    task_id="working_dir",
    python_callable=working_dir,
    dag=dag
)

complete_task =  PythonOperator(
    task_id="complete",
    python_callable=complete,
    dag=dag
)

# Configure the task dependencies such taht the graph looks like the following:
#
#                   -> current_time_task
#                 /                     \
# hello_world_task                       -> complete_task
#                 \                     /
#                   -> working_dir_task
#


hello_world_task >> current_time_task
hello_world_task >> working_dir_task
complete_task << current_time_task
complete_task << working_dir_task
