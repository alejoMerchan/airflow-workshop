import logging
import datetime
import os
import requests
import json

from airflow.models import BaseOperator
from airflow.plugins_manager import AirflowPlugin
from airflow.utils.decorators import apply_defaults


log = logging.getLogger(__name__)

class GetDataOperator(BaseOperator):

    @apply_defaults
    def __init__(self, url:str,name_file:str, *args, **kwargs):
        self.url = url
        self.name_file = name_file
        super(GetDataOperator,self).__init__(*args,**kwargs)

    def execute(self, context):
        log.info("this is my awesome operator")
        log.info("Calling Post Method")
        mypath="/data"
        if not os.path.exists(mypath):
            os.makedirs(mypath,mode=0o777)
            print("Path is created")
        response = requests.get(f'{self.url}')
        with open(f'/data/{self.name_file}', mode = 'w') as file:
         str = json.dumps(response.json())
         file.write(str)

        f = f'/data/{self.name_file}'
        records = [json.loads(line) for line in open(f)]
        records[0]

        size = os.path.getsize(f'/data/{self.name_file}')
        print(f"size:{size} ")



class GetDataPlugin(AirflowPlugin):
    name = "get_data_plugin"
    operators = [GetDataOperator]
