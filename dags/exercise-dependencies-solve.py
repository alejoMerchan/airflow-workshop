import datetime
import logging
import os
import requests
import json

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator

def my_first_pipeline():
    logging.info("starting my first pipeline")

def get_data_1():
    logging.info("Calling Post Method")
    mypath="/data"
    if not os.path.exists(mypath):
        os.makedirs(mypath,mode=0o777)
        print("Path is created")
    response = requests.get('https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2017')
    with open('/data/data1.json', mode = 'w') as file:
     str = json.dumps(response.json())
     file.write(str)

    f = '/data/data1.json'
    records = [json.loads(line) for line in open(f)]
    records[0]

    size = os.path.getsize('/data/data1.json')
    print(f"size:{size} ")

def get_data_2():
    logging.info("Calling Post Method")
    mypath="/data"
    if not os.path.exists(mypath):
        os.makedirs(mypath,mode=0o777)
        print("Path is created")
    response = requests.get('https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2018')
    with open('/data/data2.json', mode = 'w') as file:
     str = json.dumps(response.json())
     file.write(str)

    f = '/data/data2.json'
    records = [json.loads(line) for line in open(f)]
    records[0]

    size = os.path.getsize('/data/data2.json')
    print(f"size:{size} ")

def get_data_3():
    logging.info("Calling Post Method")
    mypath="/data"
    if not os.path.exists(mypath):
        os.makedirs(mypath,mode=0o777)
        print("Path is created")
    response = requests.get('https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2019')
    with open('/data/data3.json', mode = 'w') as file:
     str = json.dumps(response.json())
     file.write(str)

    f = '/data/data3.json'
    records = [json.loads(line) for line in open(f)]
    records[0]

    size = os.path.getsize('/data/data3.json')
    print(f"size:{size} ")


dag = DAG(
    "my_awesome_datapipeline",
    schedule_interval='0 12 * * *',
    start_date=datetime(2020,2,20), catchup=False
)

call_spark_job = SimpleHttpOperator(
    task_id='post_perator',
    http_conn_id='my_http_connection',
    endpoint='api/notebook/job/2F1XXKCX7',
    dag=dag
)

get_data_task_1=PythonOperator(
    task_id="get_data_task_1",
    python_callable=get_data_1,
    dag=dag
)

get_data_task_2=PythonOperator(
    task_id="get_data_task_2",
    python_callable=get_data_2,
    dag=dag
)

get_data_task_3=PythonOperator(
    task_id="get_data_task_3",
    python_callable=get_data_3,
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
