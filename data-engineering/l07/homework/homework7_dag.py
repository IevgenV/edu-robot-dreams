import os
import sys
from airflow.models import Variable

DAGBAGS_DIR = Variable.get("DAGBAGS_DIR", default_var="/home/user/airflow/dags/")
HW_DIR = os.path.join(DAGBAGS_DIR, "prod")
sys.path.append(HW_DIR)

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator

from prod.hw_utils import load_dshop, load_oos


DATA_PATH = "/home/user/datalake"  # Path to the directory where the out-of-stock data has to be stored/cached
CFG_PATH = os.path.join(HW_DIR, "main.yml")  # Name of the file with YAML configuration of the API server
SERVER_NAME = "prod_server"  # Name of the section in YAML file to take configuration from

PG_CREDS = {
      'host': 'localhost'
    , 'port': '5432'
    , 'database': 'dshop'
    , 'user': 'pguser'
    , 'password': 'secret'
}

default_args = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    dag_id='homework_dag',
    description='Download and Cache OOS Data from remote API',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1, 16),
    default_args=default_args
)

t1 = PythonOperator(
    task_id='load_oos_task',
    python_callable=load_oos,
    provide_context=True,
    op_kwargs={
        "cache_path": DATA_PATH,
        "cfg_path": CFG_PATH,
        "server_name": SERVER_NAME
    },
    dag=dag
)

t2 = PythonOperator(
    task_id='load_dshop_task',
    python_callable=load_dshop,
    op_kwargs={
        "data_path": DATA_PATH,
        "creds": PG_CREDS
    },
    dag=dag
)

t1 >> t2