from pyspark.sql import SparkSession
import os, uuid
import pandas as pd
import pyspark.sql.functions as F
from pyspark.sql.types import *

was_url = "wasbs://{0}@{1}.blob.core.windows.net/{2}"
config = {
    "fs.azure.account.key.{0}.blob.core.windows.net": "{0}",
    "fs.wasbs.impl": "org.apache.hadoop.fs.azure.NativeAzureFileSystem",
}
spark = SparkSession.builder.getOrCreate()
logger = spark.sparkContext._jvm.org.apache.log4j
storage_account = str(os.getenv('STORAGE_ACCOUNT'))
storage_account_key = str(os.getenv('STORAGE_ACCOUNT_KEY'))
container_name = str(os.getenv('AZURE_CONTAINER'))
for key in config.keys():
    if  '{' in key:
        spark.conf.set(key.format(storage_account),
                   config[key].format(storage_account_key))
    else:
        spark.conf.set(key,config[key])
print("partitions" + spark.conf.get("spark.sql.shuffle.partitions"))


# Auxiliar functions
def equivalent_type(f):
    if f == 'datetime64[ns]':
        return TimestampType()
    elif f == 'int64':
        return LongType()
    elif f == 'int32':
        return IntegerType()
    elif f == 'float64':
        return FloatType()
    else:
        return StringType()


def get_logger():
    return logger  # logger.LogManager.getLogger(__name__)


def define_structure(string, format_type):
    try:
        typo = equivalent_type(format_type)
    except:
        typo = StringType()
    return StructField(string, typo)


# Given pandas dataframe, it will return a spark's dataframe.
def pandas_to_spark(pandas_df):
    columns = list(pandas_df.columns)
    types = list(pandas_df.dtypes)
    struct_list = []
    for column, typo in zip(columns, types):
        struct_list.append(define_structure(column, typo))
    p_schema = StructType(struct_list)
    return spark.createDataFrame(pandas_df, p_schema)


def get_spark_session():
    return spark


def write_to_blob(pandas_df, file_name):
    spark_df = pandas_to_spark(pandas_df)
    print(spark_df.columns)
    write_spark_df_to_blob(spark_df,file_name)

def write_spark_df_to_blob(spark_df, file_name):
    spark_df.write.parquet(was_url.format(container_name,storage_account,file_name))

def read_spark_df_to_blob( file_name):
    spark_df = spark.read.parquet(was_url.format(container_name,storage_account,file_name))
    return spark_df


def flatten(df):
    complex_fields = dict([(field.name, field.dataType)
                           for field in df.schema.fields
                           if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    while len(complex_fields) != 0:
        col_name = list(complex_fields.keys())[0]
        print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

        # if StructType then convert all sub element to columns.
        # i.e. flatten structs
        if (type(complex_fields[col_name]) == StructType):
            expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in complex_fields[col_name]]]
            df = df.select("*", *expanded).drop(col_name)
        elif (type(complex_fields[col_name]) == ArrayType):
            df = df.withColumn(col_name, F.explode_outer(col_name))

        # recompute remaining Complex Fields in Schema
        complex_fields = dict([(field.name, field.dataType)
                               for field in df.schema.fields
                               if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
    return df


def read_blob(blob_client, file_path):
    file_content = blob_client.download_blob().readall()

    # for nested blobs, create local path as well!
    os.makedirs(os.path.dirname(file_path), exist_ok=True)

    with open(file_path, "wb") as file:
        file.write(file_content)
    df = pd.read_parquet(file_path)

    print(df.head)
