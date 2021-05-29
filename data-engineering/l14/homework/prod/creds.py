import json

from airflow.hooks.base_hook import BaseHook


## You need to have following Connections in Airflow Admin:
CONN_OOS_ID = "prod_server"
CONN_PG_ID = "dshop_db_server"
CONN_HDFS_ID = "hdfs_server"
CONN_SPARK_ID = "spark_server"

def get_oos_connection_id() -> str:
    return CONN_OOS_ID

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

def get_pg_connection_id() -> str:
    return CONN_PG_ID

def get_pg_creds() -> dict:
    conn_dshop = BaseHook.get_connection(CONN_PG_ID)
    return {
          'host': conn_dshop.host
        , 'port': conn_dshop.port
        , 'database': "dshop"
        , 'user': conn_dshop.login
        , 'password': conn_dshop.password
    }

def get_hdfs_connection_id() -> str:
    return CONN_HDFS_ID

def get_hdfs_creds() -> dict:
    conn_hdfs = BaseHook.get_connection(CONN_HDFS_ID)
    return {
          'url': ":".join([conn_hdfs.host, str(conn_hdfs.port)]),
          'user': conn_hdfs.login
    }

def get_spark_connection_id() -> str:
    return CONN_SPARK_ID

def get_spark_creds() -> dict:
    conn_spark = BaseHook.get_connection(CONN_SPARK_ID)
    return {
          'master': conn_spark.host
    }