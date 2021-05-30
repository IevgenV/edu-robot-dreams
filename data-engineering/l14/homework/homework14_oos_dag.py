import os
import sys

from airflow.models import Variable

DAGBAGS_DIR = Variable.get("DAGBAGS_DIR", default_var="/home/ivagin/airflow/dags/")
HW_DIR = os.path.join(DAGBAGS_DIR, "prod")
sys.path.append(HW_DIR)

from datetime import datetime

from airflow import DAG
from airflow.hooks.base_hook import BaseHook
from airflow.operators.python_operator import PythonOperator

from prod.creds import get_oos_connection_id, get_oos_creds, get_hdfs_creds, get_spark_creds
from prod.bronze_utils import load_oos, get_hdfs_cache_strategy
from prod.silver_utils import clean_oos


DATA_PATH_OOS_BRONZE = "/bronze/oos"  # Path to the directory where the out-of-stock data has to be stored/cached in json
DATA_PATH_OOS_SILVER = "/silver/oos"  # Path to the directory where the out-of-stock data has to be stored/cached in parquet

DEFAULT_ARGS = {
      'owner': 'airflow'
    , 'email': ['airflow@airflow.com']
    , 'email_on_failure': False
    , 'retries': 2
}

dag = DAG(
      dag_id='hw_14_oos_dag_03'
    , description='Load OOS Data from remote API to HDFS Bronze, clear and verify, then put into the Silver'
    , schedule_interval='@daily'
    , start_date=datetime(2021, 1, 1, 23)  # <- load data each evening at 11 p.m.
    , default_args=DEFAULT_ARGS
)

oos_load_task = PythonOperator(
    task_id='load_oos_bronze',
    python_callable=load_oos,
    provide_context=True,
    op_kwargs={
        "server_name": get_oos_connection_id(),
        "cache_path": DATA_PATH_OOS_BRONZE,
        "creds": get_oos_creds,
        "cache_strategy": get_hdfs_cache_strategy(get_hdfs_creds)
    },
    dag=dag
)

oos_clean_task = PythonOperator(
    task_id='clean_oos_silver',
    python_callable=clean_oos,
    provide_context=True,
    op_kwargs={
        "bronze_path": DATA_PATH_OOS_BRONZE,
        "silver_path": DATA_PATH_OOS_SILVER,
        "creds": get_spark_creds,
        "hdfs_creds": get_hdfs_creds
    },
    dag=dag
)

oos_load_task >> oos_clean_task