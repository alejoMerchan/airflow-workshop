from datetime import datetime
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators import MyFirstOperator

dag = DAG('demo_my_operator', description='another tutorial dag',
          schedule_interval='0 12 * * *',
          start_date=datetime(2020,2,20))

dummy_task = DummyOperator(task_id='dummy_task', dag=dag)

operator_task = MyFirstOperator(my_operator_param='this is a test. ',
                                task_id='my_first_operator_task', dag=dag)

dummy_task >> operator_task
