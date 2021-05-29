import logging
import os
import pathlib
from datetime import date
from typing import Callable

import psycopg2
from hdfs import InsecureClient

from .FileDailyCache import FileDailyCache
from .HDFSDailyCache import HDFSDailyCache
from .Cache import Cache
from .Server import DailyDataSource, ProdServer

DEFAULT_DATA_PATH = "~"
DEFAULT_PG_CREDS = {
      'host': 'localhost'
    , 'port': '5432'
    , 'database': 'dshop'
    , 'user': 'pguser'
    , 'password': ''
}
DEFAULT_TABLE_NAMES = ["aisles", "clients", "departments", "orders", "products"]

DEFAULT_OOS_CONFIG = {
    "prod_server": {
        "address": "https://robot-dreams-de-api.herokuapp.com",
        "login": "rd_dreams",
        "password": "djT6LasE",
        "apis": {
            "authorize": {
               "endpoint": "/auth",
               "method": "POST"
            },
            "out_of_stock": {
                "endpoint": "/out_of_stock",
                "method": "GET"
            }
        }
    }
}

def get_filesystem_cache_strategy():
    def create_local_fs_cache(cache_dir:pathlib.Path,
                              data_source:DailyDataSource,
                              cache_date:date=None) -> Cache:
        logging.info(f"Creating local filesystem cache to store OOS data...")
        cache = FileDailyCache(pathlib.Path(cache_dir), data_source, cache_date)
        logging.info(f"Local filesystem cache object for storing OOS data has been created.")
        return cache
    return create_local_fs_cache

def get_hdfs_cache_strategy(creds_gen:Callable[..., dict]):
    def create_hdfs_cache(cache_dir:pathlib.Path,
                          data_source:DailyDataSource,
                          cache_date:date=None) -> Cache:
        creds = creds_gen()
        logging.info(f"Create client and connect to HDFS at {creds['url']}...")
        hdfs_client = InsecureClient(**creds)
        logging.info(f"Client has been created.")
        logging.info(f"Creating HDFS cache to store OOS data...")
        cache = HDFSDailyCache(hdfs_client, pathlib.Path(cache_dir), data_source, cache_date)
        logging.info(f"HDFS cache object for storing OOS data has been created.")
        return cache
    return create_hdfs_cache

def load_oos(**kwargs):
    logging.info("Parameters for OOS products loading parsing started...")
    cache_directory = kwargs.get("cache_path")
    config = kwargs.get("cfg_path")
    oos_creds = kwargs.get("creds")
    server_name = kwargs.get("server_name")
    cache_strategy = kwargs.get("cache_strategy")
    cache_date = kwargs.get("execution_date")

    cache_directory = DEFAULT_DATA_PATH if cache_directory is None else pathlib.Path(cache_directory)
    config = config if oos_creds is None else oos_creds()
    config = DEFAULT_OOS_CONFIG if config is None else config
    cache_date = date.today() if cache_date is None else cache_date.date()
    DEFAULT_CACHE_STRATEGY = get_filesystem_cache_strategy()
    cache_strategy = DEFAULT_CACHE_STRATEGY if cache_strategy is None else cache_strategy

    logging.info("Parameters for OOS products loading were parsed:")
    logging.info(f"Target data root path: {cache_directory}")
    logging.info(f"Target data path: {cache_date.isoformat()}")
    logging.info(f"Data caching strategy: {cache_strategy.__name__}")

    logging.info("Creating data source (connection with remote API):")
    server = ProdServer(config, server_name)
    logging.info("Data source (i.e., connection with remote API) has been created.")

    logging.info("Create Cache object to store retrieved product data into directory:")
    cache = cache_strategy(pathlib.Path(cache_directory), server, cache_date)
    logging.info("Cache object has been created.")

    logging.info("Attempt to cache data from data source...")
    if cache.update():  # <- Gets data and cache it at disk if not cached yet
        logging.info("Data has been cached.")
        return "Data has been saved (cached) to the {}".format(cache.get_cache_filepath())
    else:
        logging.error("Data has NOT been cached.")
        msg = "Data hasn't been retrieved for {} date. " \
              "Seems that it isn't available at data source".format(cache_date.isoformat())
        raise ResourceWarning(msg)

def get_load_table_to_os_strategy():
    def load_table_to_local_os(data_path:str, db_creds:dict, table_name:str):
        logging.info(f"Started loading DB table into the local filesystem: {data_path}...")
        if not os.path.exists(data_path):
            try:
                logging.info(f"{data_path} directory doesn't exist. Creating directory tree...")
                os.makedirs(data_path)
                logging.info(f"{data_path} directory tree has been created.")
            except OSError:
                logging.error(f"{data_path} directory tree can not be created.")
                raise OSError(f"Can't create data directory: `{data_path}`")

        logging.info("Connecting to the database. "
                    f"host: {db_creds['host']}, "
                    f"port: {db_creds['port']}, "
                    f"db: {db_creds['database']} ...")
        with psycopg2.connect(**db_creds) as pg_connection:
            logging.info("Connection to the database has been established")
            cursor = pg_connection.cursor()
            logging.info("Local file opening for writing...")
            local_filename = os.path.join(data_path, f"{table_name}.csv")
            with open(file=local_filename, mode='w') as csv_file:
                logging.info("Local file has been opened.")
                logging.info(f"Writing data from `{table_name}` DB table to {local_filename}...")
                cursor.copy_expert(f"COPY public.{table_name} TO STDOUT WITH HEADER CSV", csv_file)
                logging.info(f"Table `{table_name}` has been dumped top the {local_filename}.")
        logging.info(f"Loading DB table to the local filesystem at {data_path} has been finished.")
    return load_table_to_local_os

def get_load_table_to_hdfs_strategy(creds_gen:Callable[..., dict]):
    def load_table_to_hdfs(data_path:str, db_creds:dict, table_name:str):
        logging.info(f"Started loading DB table into the HDFS: {data_path}...")
        creds = creds_gen()
        logging.info(f"Create client and connect to HDFS at {creds['url']}...")
        hdfs_client = InsecureClient(**creds)
        logging.info(f"Client has been created.")
        if hdfs_client.status(data_path, strict=False) is None:
            try:
                logging.info(f"{data_path} directory doesn't exist. Creating directory tree...")
                hdfs_client.makedirs(data_path)
                logging.info(f"{data_path} directory tree has been created.")
            except OSError:
                logging.error(f"{data_path} directory tree can not be created.")
                raise OSError("Can't create HDFS data directory: '{}'".format(data_path))

        hdfs_fname = os.path.join(data_path, f"{table_name}.csv")
        logging.info("Connecting to the database. "
                    f"host: {db_creds['host']}, "
                    f"port: {db_creds['port']}, "
                    f"db: {db_creds['database']} ...")
        with psycopg2.connect(**db_creds) as pg_connection:
            logging.info("Connection to the database has been established")
            cursor = pg_connection.cursor()
            logging.info("HDFS file opening for writing...")
            with hdfs_client.write(hdfs_fname, overwrite=True) as csv_file:
                logging.info("HDFS file has been opened.")
                logging.info(f"Writing data from `{table_name}` DB table to {hdfs_fname}...")
                cursor.copy_expert(f"COPY public.{table_name} TO STDOUT WITH HEADER CSV", csv_file)
                logging.info(f"Table `{table_name}` has been dumped top the {hdfs_fname}.")
        logging.info(f"Loading DB table into the HDFS at {data_path} has been finished.")
    return load_table_to_hdfs

def load_dshop_table(**kwargs):
    table_name = kwargs.get("table_name")
    if table_name is None:
        logging.error("Table name has to be specified, but it is not.")
        raise ValueError("Table name has to be specified.")

    logging.info("Parameters for table loading parsing started...")
    data_path = kwargs.get("data_path")
    db_creds = kwargs.get("creds")
    cache_date = kwargs.get("execution_date")
    load_strategy = kwargs.get("load_strategy")

    data_path = DEFAULT_DATA_PATH if data_path is None else data_path
    db_creds = DEFAULT_PG_CREDS if db_creds is None else db_creds()
    cache_date = date.today() if cache_date is None else cache_date.date()
    DEFAULT_LOAD_STRATEGY = get_load_table_to_os_strategy()
    load_strategy = DEFAULT_LOAD_STRATEGY if load_strategy is None else load_strategy

    logging.info("Parameters for table loading were parsed:")
    logging.info(f"Target data root path: {data_path}")
    logging.info(f"Target data path: {cache_date.isoformat()}")
    logging.info(f"Data loading strategy: {load_strategy.__name__}")

    data_path = os.path.join(data_path, cache_date.isoformat())
    load_strategy(data_path=data_path, db_creds=db_creds, table_name=table_name)

    return f"Table {table_name} was succesfully loaded to {data_path}"

def load_dshop(**kwargs):
    result_messages = []

    logging.info("Running loading tables from database. All tables will be loaded in one task...")
    for table_name in DEFAULT_TABLE_NAMES:
        result_message = load_dshop_table(kwargs, table_name=table_name)
        result_messages.append(result_message)
    logging.info("Table loading task is finished.")

    return "\n".join(result_messages)
