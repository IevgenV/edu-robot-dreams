import logging
import abc
import pathlib
from datetime import date
from typing import Union, overload
from pyspark.sql.dataframe import DataFrame

import pyspark.sql.functions as F
from pyspark.sql import SparkSession
from hdfs import InsecureClient


DEFAULT_BRONZE_PATH = "/bronze"
DEFAULT_SILVER_PATH = "/silver"
DEFAULT_SPARK_MASTER = "local"
DEFAULT_HDFS_CREDS = {
          'url': "http://127.0.0.1:50070",
          'user': "user"
    }

class AbstractFlaskCleaner:
    def __init__(self
               , bronze_src_file:Union[pathlib.Path, str]
               , silver_dst_file:Union[pathlib.Path, str]
               , spark_master:str=DEFAULT_SPARK_MASTER
               , spark_app_name:str="oos_cleaner"
               , override_dst:bool=False):
        assert isinstance(bronze_src_file, pathlib.Path) \
            or isinstance(bronze_src_file, str), \
            "`bronze_src_file` argument is expected to be pathlib.Path or string"
        assert isinstance(silver_dst_file, pathlib.Path) \
            or isinstance(silver_dst_file, str), \
            "`silver_dst_file` argument is expected to be pathlib.Path or string"
        bronze_src_file = bronze_src_file \
                       if isinstance(bronze_src_file, pathlib.Path) \
                       else pathlib.Path(bronze_src_file) 
        silver_dst_file = silver_dst_file \
                       if isinstance(silver_dst_file, pathlib.Path) \
                       else pathlib.Path(silver_dst_file)

        supported_dst_formats = ["parquet"]
        dst_data_format = bronze_src_file.suffix.lstrip('.')
        if dst_data_format in supported_dst_formats:
            raise TypeError("Destination file at Silver need to have one "
                           f"of the supported extensions: {supported_dst_formats}.")
        
        logging.info(f"SPARK SERVER: {spark_master}")
        self.spark = SparkSession.builder \
            .master(spark_master).appName(spark_app_name).getOrCreate()
        if override_dst:
            # NOTE(i.vagin): This option has to be used when destination file is read and then ovrwtitten when `move_to_silver`:
            self.spark.conf.set('spark.sql.sources.partitionOverwriteMode', 'dynamic')
        self.src_path = bronze_src_file
        self.dst_path = silver_dst_file

    def _open_file_as_df(self, fpath:pathlib.Path) -> DataFrame:
        # NOTE(i.vagin): # Format is based on file extension:
        src_data_format = fpath.suffix.lstrip('.') 
        logging.info(f"Open `{fpath}` file as Spark DataFrame...")
        df = self.spark.read.format(src_data_format).load(fpath.as_posix())
        records_cnt = df.count()
        logging.info(f"`{fpath}` has been opened."
                     f" DataFrame contains {records_cnt} records.")
        return df

    @abc.abstractmethod
    def clean(self) -> DataFrame:
        pass

    def move_to_silver(self, mode:str="append", df:DataFrame=None) -> DataFrame:
        if df is None:
            logging.info("Start data cleaning process...")
            df = self.clean()
            logging.info("Data cleaning done.")
        rows_number = df.count()
        logging.info(f"Saving cleaned data into `{self.dst_path}`. {rows_number} rows are going to be written.")
        # NOTE(i.vagin): # Format is based on file extension:
        dst_data_format = self.dst_path.suffix.lstrip('.')
        df.write.format(dst_data_format).mode(mode).save(self.dst_path.as_posix())
        logging.info(f"Cleaned data has been wtited to `{self.dst_path}` in `{mode}` mode.")
        return df


class OOSCleaner(AbstractFlaskCleaner):
    def __init__(self
               , bronze_src_file:Union[pathlib.Path, str]
               , silver_dst_file:Union[pathlib.Path, str]
               , spark_master:str=DEFAULT_SPARK_MASTER
               , spark_app_name:str="oos_cleaner"):
        super(OOSCleaner, self).__init__(bronze_src_file
                                       , silver_dst_file
                                       , spark_master
                                       , spark_app_name
                                       , override_dst=False)

    def clean(self) -> DataFrame:
        logging.info("Load and validate data source before cleaning...")
        df_src = self._open_file_as_df(self.src_path)
        logging.info(f"Schema: {df_src.schema}...")
        try:
            df_dst = self._open_file_as_df(self.dst_path)
        except:
            logging.info(f"Can't open `{self.dst_path}` file. Will use empty dst Silver data.")
            df_dst = None
        logging.info("Start source data cleaning...")
        orig_rows_cnt = df_src.count()
        df_src = df_src.dropna()
        unmerged_rows_cnt = df_src.count()
        rows_cleaned_cnt = orig_rows_cnt - unmerged_rows_cnt
        logging.info("Source has been cleaned.")
        logging.info(f"Merge data from bronze `{self.src_path}` to silver `{self.dst_path}` with duplicates removing...")
        df = df_src if df_dst is None else df_src.join(df_dst, on=df_src.columns, how="left_outer") \
                                                 .filter(df_dst.date.isNull()) \
                                                 .select(df_src.columns)
        df = df.dropDuplicates()
        duplicates_cnt = unmerged_rows_cnt - df.count()
        orig_rows_cnt = df.count() + (0 if df_dst is None else df_dst.count())
        logging.info(f"Data has been merged. Total rows after merge: {orig_rows_cnt}")
        logging.info(f"Data cleaning for `{self.src_path}` bronze data source is done. "
                     f"{rows_cleaned_cnt} invalid rows and {duplicates_cnt} duplicates were dropped.")
        return df


class DShopCleaner(AbstractFlaskCleaner):
    def __init__(self
               , bronze_src_file:Union[pathlib.Path, str]
               , silver_dst_file:Union[pathlib.Path, str]
               , spark_master:str=DEFAULT_SPARK_MASTER
               , spark_app_name:str="dshop_cleaner"):
        super(DShopCleaner, self).__init__(bronze_src_file
                                         , silver_dst_file
                                         , spark_master
                                         , spark_app_name)

    def clean(self) -> DataFrame:
        df = self._open_file_as_df(self.src_path)
        logging.info("We don't clean data for DShop tables because they came from PG tables "
                    f"and data is expected to be clean already. Just replace old `{self.dst_path}` "
                    f"with content of `{self.src_path}` from Bronze.")
        return df


def _parse_common_arguments(**kwargs):
    bronze_path = kwargs.get("bronze_path")
    silver_path = kwargs.get("silver_path")
    spark_creds = kwargs.get("creds")
    hdfs_creds  = kwargs.get("hdfs_creds")
    bronze_date = kwargs.get("execution_date")

    bronze_path = DEFAULT_BRONZE_PATH if bronze_path is None else bronze_path
    bronze_path = pathlib.Path(str(bronze_path))
    silver_path = DEFAULT_SILVER_PATH if silver_path is None else silver_path
    silver_path = pathlib.Path(str(silver_path))

    spark_master = DEFAULT_SPARK_MASTER if spark_creds is None else spark_creds().get("master")
    spark_master = DEFAULT_SPARK_MASTER if spark_master is None else spark_master

    hdfs_creds = DEFAULT_HDFS_CREDS if hdfs_creds is None else hdfs_creds()

    bronze_date = date.today() if bronze_date is None else bronze_date.date()

    bronze_path = bronze_path / bronze_date.isoformat()

    return bronze_path, silver_path, spark_master, hdfs_creds


def _hdfs_files_and_directories(hdfs_creds, bronze_file, silver_path):
    hdfs_client = InsecureClient(**hdfs_creds)
    if hdfs_client.status(bronze_file, strict=False) is None:
        logging.error(f"`{bronze_file}` doesn't exist at Bronze.")
        raise OSError(f"Can't find `{bronze_file}` file at HDFS.")
    if hdfs_client.status(silver_path, strict=False) is None:
            try:
                logging.info(f"`{silver_path}` directory doesn't exist. Creating directory tree...")
                hdfs_client.makedirs(silver_path)
                logging.info(f"`{silver_path}` directory tree has been created.")
            except OSError:
                logging.error(f"`{silver_path}` directory tree can not be created.")
                raise OSError(f"Can't create HDFS data directory: `{silver_path}`")


def clean_oos(**kwargs):
    logging.info("Parameters for OOS clean parsing started...")

    bronze_path, silver_path, spark_master, hdfs_creds = _parse_common_arguments(**kwargs)
    bronze_file = bronze_path / pathlib.Path(".".join([bronze_path.stem, "json"]))
    silver_file = silver_path / pathlib.Path("oos.parquet")

    logging.info("Parameters for OOS clean parsing is done.")

    logging.info("Check if OOS files and directories are existed at HDFS.")
    _hdfs_files_and_directories(hdfs_creds, bronze_file, silver_path)
    logging.info("Check of OOS files and directories existance at HDFS done.")

    logging.info("Cleaning data...")
    cleaner = OOSCleaner(bronze_file, silver_file, spark_master)
    cleaner.move_to_silver()
    logging.info("Cleaning data done")


def clean_dshop(**kwargs):
    logging.info("Parameters for DDhop clean parsing started...")

    table_name = kwargs.get("table_name")
    if table_name is None:
        logging.error("Table name has to be specified, but it is not.")
        raise ValueError("Table name has to be specified.")

    bronze_path, silver_path, spark_master, hdfs_creds = _parse_common_arguments(**kwargs)
    bronze_file = bronze_path / pathlib.Path(".".join([table_name, "csv"]))
    silver_file = silver_path / pathlib.Path(f"{table_name}.parquet")

    logging.info("Parameters for DDhop clean parsing is done.")

    logging.info("Check if DShop files and directories are existed at HDFS.")
    _hdfs_files_and_directories(hdfs_creds, bronze_file, silver_path)
    logging.info("Check of DShop files and directories existance at HDFS done.")

    logging.info("Cleaning data...")
    cleaner = DShopCleaner(bronze_file, silver_file, spark_master)
    cleaner.move_to_silver()
    logging.info("Cleaning data done")