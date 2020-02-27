import datetime
import logging
import os
import requests
import json

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator
from airflow.operators import GetDataOperator

def my_first_pipeline():
    logging.info("starting my first pipeline")


dag = DAG(
    "my_awesome_datapipeline-2",
    schedule_interval='0 12 * * *',
    start_date=datetime(2020,2,20), catchup=False
)

call_spark_job = SimpleHttpOperator(
    task_id='post_perator',
    http_conn_id='my_http_connection',
    endpoint='api/notebook/job/2F1XXKCX7',
    dag=dag
)

get_data_task_1=GetDataOperator(
    task_id="get_data_task_1",
    url='https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2017',
    name_file="data1.json",
    dag=dag
)

get_data_task_2=GetDataOperator(
    task_id="get_data_task_2",
    url='https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2018',
    name_file="data2.json",
    dag=dag
)

get_data_task_3=GetDataOperator(
    task_id="get_data_task_3",
    url='https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2019',
    name_file="data3.json",
    dag=dag
)

my_first_datapipeline=PythonOperator(
    task_id="my_first_pipeline",
    python_callable=my_first_pipeline,
    dag=dag
)

my_first_datapipeline >> get_data_task_1
my_first_datapipeline >> get_data_task_2
my_first_datapipeline >> get_data_task_3
call_spark_job << get_data_task_1
call_spark_job << get_data_task_2
call_spark_job << get_data_task_3

# Create Tasks and Operators

#get_data_task_2
#get_data_task_3
#call_spark_job


# Configure the task dependencies such taht the graph looks like the following:
#
#                   -> get_data_task_1 (2017)
#                 /                        \
# my_first_datapipeline -> get_data_task_2 (2018)  -> call_spark_job
#                 \                        /
#                   -> get_data_task_3 (2019)
#
