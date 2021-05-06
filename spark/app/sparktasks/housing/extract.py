import os
from pyspark.sql import SparkSession
import pandas as pd
from sparktasks.utils.DBUtils import DButils
from sparktasks.utils.utils import UdfUtils
from sparktasks.utils.config import Config
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType
import logging
import time


class Extract:
    logger = logging.getLogger('sparktasks.housing.Extract')

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('HousingExtract').getOrCreate()
        self.config = Config()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.config.metadata)
        self.metadata_df.createGlobalTempView(self.config.metadata)

    # Extracts csv files from zillow.com
    def extract_from_source(self):  # ,**kwargs):
        # ti = kwargs['ti']
        # metadata_dictionary = ti.xcom_pull(task_ids='extract_metadata',key="dag_last_run_details")
        housing_price = self.config.housing_price
        print(housing_price)
        self.extract(self.config.housing_price)
        housing_inventory = self.config.housing_inventory
        self.extract(self.config.housing_inventory)

    # saves in local folder.Gets previous run from metadata and discards
    # old data.
    def extract(self, source_map):
        try:
            housing_dict = dict(source_map)
            for key, value in housing_dict.items():
                logging.info("Data Extract in progress from %s", value)
                housing_data = pd.read_csv(value)
                data_dir = self.config.data_dir
                full_path = os.path.join(data_dir, key + ".csv")
                housing_data.to_csv(full_path, index=False)
                logging.info("Data Extracted to %s", full_path)
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex

    # Stores CSV in landing database as raw tables
    def store_raw_in_db(self):
        self.write_raw(self.config.housing_price)
        split_udf = udf(lambda d: UdfUtils.split_str(d), StringType())
        self.write_raw(self.config.housing_inventory, split_udf)

    def write_raw(self, type_config, split_udf=None):
        housing_dict = dict(type_config)
        data_dir = self.config.data_dir
        for name, value in housing_dict.items():
            try:
                start_time = time.time()
                housing_path = os.path.join(data_dir, name + ".csv")
                logging.info("Started to create Raw table from %s", name)
                file_name = os.path.basename(housing_path)
                self.spark.sparkContext.addFile(housing_path)
                housing_us_df = self.spark.read.csv('file://' + housing_path, header=True, inferSchema=True)
                housing_us_df.filter(housing_us_df.StateName.isNull())
                if split_udf is not None:
                    housing_us_df = housing_us_df.withColumn("RegionName", split_udf(housing_us_df.RegionName))
                print(housing_us_df.columns)
                housing_us_df = housing_us_df.fillna(0)
                if housing_us_df.count()==0:
                    return

                table_name = self.config.get_config('RAW', name)
                self.DButils.save_to_db(housing_us_df,table_name, mode='overwrite' )
                logging.info("Created Raw table name %s", table_name)
                end_time = time.time()
                print("It took this long to run write_raw: {}".format(end_time-start_time))
                logging.info("It took this long to run write_raw: {}".format(end_time-start_time))
            except Exception as ex:
                logging.error("Error in store_raw_in_db %s", ex)
                raise ex

if __name__ == "__main__":
    extract = Extract()
    extract.extract_from_source()
    extract.store_raw_in_db()