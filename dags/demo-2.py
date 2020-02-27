import logging
import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def hello_world():
    logging.info("hellow world !")


dag = DAG(
        'demo2',
         start_date=datetime.datetime.now() - datetime.timedelta(days=5),
         schedule_interval='@daily')


my_first_task=PythonOperator(
    task_id="my_first_task",
    python_callable=hello_world,
    dag=dag
)
