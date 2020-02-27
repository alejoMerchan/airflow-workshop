from datetime import datetime
from airflow import DAG
from airflow.operators.http_operator import SimpleHttpOperator

## http://airflow.apache.org/docs/stable/_api/airflow/operators/http_operator/index.html


dag = DAG('demo_simplehttp_operator', start_date=datetime(2020,2,27))

simple_http_operator_task = SimpleHttpOperator(
    task_id='post_perator',
    http_conn_id='my_http_connection',
    endpoint='api/notebook/job/2F2XANJRH',
    dag=dag
)
