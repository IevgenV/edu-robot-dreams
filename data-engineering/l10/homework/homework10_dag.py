import os
import sys
import json
from typing import Callable

from airflow.models import Variable

DAGBAGS_DIR = Variable.get("DAGBAGS_DIR", default_var="/home/ivagin/airflow/dags/")
HW_DIR = os.path.join(DAGBAGS_DIR, "prod")
sys.path.append(HW_DIR)

from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from prod.hw_utils import load_dshop_table, load_oos, \
                          get_hdfs_cache_strategy, get_load_table_to_hdfs_strategy


DATA_PATH_OOS = "/bronze/oos"  # Path to the directory where the out-of-stock data has to be stored/cached
DATA_PATH_DSHOP = "/bronze/dshop"  # Path to the directory where the dshop tables have to be stored into csv

## You need to create following Connections in Airflow Admin:
CONN_PG_ID = "dshop_db_server"
CONN_OOS_ID = "prod_server"
CONN_HDFS_ID = "hdfs_server"

def get_pg_creds() -> dict:
    conn_dshop = BaseHook.get_connection(CONN_PG_ID)
    return {
          'host': conn_dshop.host
        , 'port': conn_dshop.port
        , 'database': "dshop"
        , 'user': conn_dshop.login
        , 'password': conn_dshop.password
    }

def get_oos_creds() -> dict:
    conn_oos = BaseHook.get_connection(CONN_OOS_ID)
    return {
          CONN_OOS_ID : {
              'address': conn_oos.host,
              'login': conn_oos.login,
              'password': conn_oos.password,
              'apis': json.loads(conn_oos.extra)
          }
    }

def get_hdfs_creds() -> dict:
    conn_hdfs = BaseHook.get_connection(CONN_HDFS_ID)
    return {
          'url': ":".join([conn_hdfs.host, str(conn_hdfs.port)]),
          'user': conn_hdfs.login
    }

PG_TABLE_NAMES = ["aisles", "clients", "departments", "orders", "products"]

DEFAULT_ARGS = {
    'owner': 'airflow',
    'email': ['airflow@airflow.com'],
    'email_on_failure': False,
    'retries': 2
}

dag = DAG(
    dag_id='hw_10_dag',
    description='Download and Cache OOS Data from remote API to HDFS0 /bronze',
    schedule_interval='@daily',
    start_date=datetime(2021, 1, 1, 23),  # <- load data each evening
    default_args=DEFAULT_ARGS
)

oos_task = PythonOperator(
    task_id='load_oos_task',
    python_callable=load_oos,
    provide_context=True,
    op_kwargs={
        "server_name": CONN_OOS_ID,
        "cache_path": DATA_PATH_OOS,
        "creds": get_oos_creds,
        "cache_strategy": get_hdfs_cache_strategy(get_hdfs_creds)
    },
    dag=dag
)

def create_pytask(dag:DAG, task_id:str, python_func:Callable, *args, **kwargs):
    return PythonOperator(
        task_id=task_id,
        python_callable=python_func,
        provide_context=True,
        op_args=args,
        op_kwargs=kwargs,
        dag=dag
    )

for table_name in PG_TABLE_NAMES:
    oos_task >> create_pytask(dag, f"load_{table_name}_table", load_dshop_table,
                              table_name=table_name, data_path=DATA_PATH_DSHOP,
                              creds=get_pg_creds, \
                              load_strategy=get_load_table_to_hdfs_strategy(get_hdfs_creds))
