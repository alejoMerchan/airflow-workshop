import logging

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("hellow world !")


dag = DAG(
        'demo1',
         start_date=datetime(2020,2,20))


my_first_task=PythonOperator(
    task_id="my_first_task",
    python_callable=hello_world,
    dag=dag
)
