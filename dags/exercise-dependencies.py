import datetime
import logging
import os
import json
import requests

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.http_operator import SimpleHttpOperator


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



dag = DAG(
    "my_awesome_datapipeline_exercise",
    schedule_interval='0 12 * * *',
    start_date=datetime(2020,2,27)
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
