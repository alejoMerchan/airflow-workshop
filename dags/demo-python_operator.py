import logging
import requests
import json
import os

from datetime import datetime
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def get_data():
    logging.info("Calling Post Method")
    mypath="/data"
    if not os.path.exists(mypath):
        os.makedirs(mypath,mode=0o777)
        print("Path is created")
    response = requests.get('https://apiocds.colombiacompra.gov.co:8443/apiCCE2.0/rest/releases/page/2019?page=1')
    if response:
        print('Success!')
    else:
        print('An error has occurred.')
        print(response.status_code)

    with open('/data/data.json', mode = 'w') as file:
        str = json.dumps(response.json())
        file.write(str)

    f = '/data/data.json'
    records = [json.loads(line) for line in open(f)]
    records[0]

    size = os.path.getsize('/data/data.json')
    print(f"size:{size} ")


dag = DAG(
    "demo_python_operator",
    schedule_interval='0 12 * * *',
    start_date=datetime(2020,2,27)
)

get_data_task=PythonOperator(
    task_id="get_data_task_demo",
    python_callable=get_data,
    dag=dag
)
