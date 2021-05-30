import os
import sys
from typing import Callable

from airflow.models import Variable

DAGBAGS_DIR = Variable.get("DAGBAGS_DIR", default_var="/home/ivagin/airflow/dags/")
HW_DIR = os.path.join(DAGBAGS_DIR, "prod")
sys.path.append(HW_DIR)

from datetime import datetime

from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from prod.creds import get_pg_creds, get_hdfs_creds, get_spark_creds
from prod.bronze_utils import load_dshop_table, get_load_table_to_hdfs_strategy
from prod.silver_utils import clean_dshop


DATA_PATH_DSHOP_BRONZE = "/bronze/dshop"  # Path to the directory where the dshop tables have to be stored in csv
DATA_PATH_DSHOP_SILVER = "/silver/dshop"  # Path to the directory where the dshop tables have to be stored in parquet

PG_TABLE_NAMES = ["aisles", "clients", "departments", "orders", "products"]

DEFAULT_ARGS = {
      'owner': 'airflow'
    , 'email': ['airflow@airflow.com']
    , 'email_on_failure': False
    , 'retries': 2
}

dag = DAG(
      dag_id='hw_14_dshop_dag_03'
    , description='Load data from `dshop` database to Bronze, clear and verify  then put into the Silver'
    , schedule_interval='@daily'
    , start_date=datetime(2021, 1, 1, 5)  # <- load data each morning at 5 a.m.
    , default_args=DEFAULT_ARGS
)

def create_pytask(dag:DAG, task_id:str, python_func:Callable, *args, **kwargs):
    return PythonOperator(
          task_id=task_id
        , python_callable=python_func
        , provide_context=True
        , op_args=args
        , op_kwargs=kwargs
        , dag=dag
    )

wait_bronze = DummyOperator(
      task_id="wait_dshop_bronze_load"
    , dag=dag
)

for table_name in PG_TABLE_NAMES:
    create_pytask(dag, f"load_{table_name}_table_bronze", load_dshop_table \
                , table_name=table_name, data_path=DATA_PATH_DSHOP_BRONZE \
                , creds=get_pg_creds \
                , load_strategy=get_load_table_to_hdfs_strategy(get_hdfs_creds)) \
    >> wait_bronze >> \
    create_pytask(dag, f"move_{table_name}_table_silver", clean_dshop \
                , table_name=table_name \
                , bronze_path=DATA_PATH_DSHOP_BRONZE, silver_path=DATA_PATH_DSHOP_SILVER \
                , creds=get_spark_creds, hdfs_creds=get_hdfs_creds)
