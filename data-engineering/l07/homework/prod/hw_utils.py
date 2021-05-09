import os
import psycopg2

import pathlib
from datetime import datetime

from .Cache import FileDailyCache
from .Server import ProdServer


PG_DEFAULT_CREDS = {
      'host': 'localhost'
    , 'port': '5432'
    , 'database': 'dshop'
    , 'user': 'pguser'
    , 'password': ''
}


def load_oos(**kwargs):
    cache_directory = pathlib.Path(kwargs["cache_path"])
    configuration_file = pathlib.Path(kwargs["cfg_path"])
    server_name = kwargs["server_name"]
    # Create ProdServer object to request remote HTTP server for out of stock products:
    server = ProdServer(configuration_file, server_name)

    cache_date = kwargs['execution_date'].date()
    # Create FileDailyCache object to store (cache) retrieved product data into directory:
    cache = FileDailyCache(cache_directory, server, cache_date)
    if cache.update():  # <- Gets data and cache it at disk if not cached yet
        return "Data has been saved (cached) to the {}".format(cache.get_cache_filepath())
    else:
        return "Data hasn't been retrieved for {} date".format(cache_date.isoformat())

def load_dshop(**kwargs):
    data_path = kwargs["data_path"]
    pg_creds = kwargs["creds"]

    data_path = "~" if data_path is None else data_path
    pg_creds = PG_DEFAULT_CREDS if pg_creds is None else pg_creds

    if not os.path.exists(data_path):
        try:
            os.makedirs(os.path.data_path)
        except OSError:
            raise OSError("Can't create data directory: '{}'".format(data_path))

    with psycopg2.connect(**pg_creds) as pg_connection:
        cursor = pg_connection.cursor()
        with open(file=os.path.join(data_path, 'aisles.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.aisles TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join(data_path, 'clients.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.clients TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join(data_path, 'departments.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.departments TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join(data_path, 'orders.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.orders TO STDOUT WITH HEADER CSV', csv_file)
        with open(file=os.path.join(data_path, 'products.csv'), mode='w') as csv_file:
            cursor.copy_expert('COPY public.products TO STDOUT WITH HEADER CSV', csv_file)