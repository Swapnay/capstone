from pyspark.sql import SparkSession
import os, uuid
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *
from pyspark import SparkConf, SparkContext
import logging

was_url = "wasbs://{0}@{1}.blob.core.windows.net/{2}"
config = {
    "fs.azure.account.key.{0}.blob.core.windows.net": "{0}",
    "fs.wasbs.impl": "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
}
__spark = SparkContext()#SparkSession.builder.master('local').appName('app').getOrCreate()
logger = logging.getLogger("sablobutils")
storage_account = str(os.getenv('STORAGE_ACCOUNT'))
storage_account_key = str(os.getenv('STORAGE_ACCOUNT_KEY'))
container_name = str(os.getenv('AZURE_CONTAINER'))
'''for key in config.keys():
    if '{' in key:
        spark.conf.set(key.format(storage_account),
                       config[key].format(storage_account_key))
    else:
        spark.conf.set(key, config[key])'''


def get_logger():
    return logger  # logger.LogManager.getLogger(__name__)


def get_spark_session():
    return __spark


def write_parquet_to_blob(spark_df, file_name, container=None):
    if container is None:
        spark_df.write.parquet(was_url.format(container_name, storage_account, file_name))
    else:
        spark_df.write.parquet(was_url.format(container, storage_account, file_name))


def read_parquet_from_blob(spark,file_name):
    spark_df = spark.read.parquet(was_url.format(container_name, storage_account,
                                                 file_name))
    return spark_df


def read_blob(blob_client, file_path):
    file_content = blob_client.download_blob().readall()

    # for nested blobs, create local path as well!
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "wb") as file:
        file.write(file_content)
    df = pd.read_parquet(file_path)

    print(df.head)


def read_raw_text_from_blob(spark, file_name,container=None):
    if container is None:
        print(was_url.format(container_name, storage_account, file_name))
        return spark.sparkContext.textFile(was_url.format(container_name, storage_account, file_name))
    else:
        return spark.sparkContext.textFile(was_url.format(container, storage_account, file_name))