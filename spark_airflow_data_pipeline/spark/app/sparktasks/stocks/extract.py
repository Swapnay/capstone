import os
from pyspark.sql import SparkSession
import pandas as pd
from spark.app.sparktasks.utils.DBUtils import DButils
from spark.app.sparktasks.utils.config import Config
import datetime
import logging
from pandas_datareader import data
import  pyspark.sql.functions as F


class Extract:
    logger = logging.getLogger('sparktasks.housing.Extract')

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('Extract').master('local').getOrCreate()
        self.config = Config()
        self.metadata_df = self.DButils.load_from_db(self.spark, self.config.metadata)
        self.metadata_df.createGlobalTempView(self.config.metadata)

    # Extracts csv files from zillow.com
   # def extract_Stocks(self):  # ,**context):
        # ti = context['ti']
        # metadata_dictionary = ti.xcom_pull(task_ids='extract_data',key="dag_last_run_details")
        #self.extract_from_source(self.config.stocks)

    # saves in local folder.Gets previous run from metadata and discards
    # old data.
    def extract_from_source(self):
        try:
            stocks_dict = dict(self.config.stocks)
            for key, value in stocks_dict.items():
                logging.info("Data Extract in progress from %s", value)
                data_dir = self.config.data_dir
                stocks_data = pd.read_csv(value)
                symbols = stocks_data["Symbol"].to_list()

                for symbol in symbols:
                    row = self.metadata_df.filter(self.metadata_df.sector_sub_type == symbol).first()
                    end_date = datetime.datetime.now()
                    if row:
                        start_date = row[4] + datetime.timedelta(days=1)
                    else:
                        start_date = '2008-01-01'

                    try:
                        panel_data = data.DataReader(symbol, 'yahoo', start_date, end_date)
                        new_path = os.path.join(self.config.data_dir, symbol + ".csv")
                        panel_data.to_csv(new_path)
                    except Exception as ex:
                        print(ex)
                full_path = os.path.join(data_dir, key)
                stocks_data.to_csv(full_path, index=False)
                logging.info("Data Extracted to %s", full_path)
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex

    # Stores CSV in landing database as raw tables
    def store_raw_in_db(self):
        self.write_raw(self.config.stocks)

    def write_raw(self, stocks):
        try:
            stocks_dict = dict(stocks)
            table_name = self.config.stocks_raw
            for key, value in stocks_dict.items():
                data_dir = self.config.data_dir
                stocks_data = pd.read_csv(value)
                symbols = stocks_data["Symbol"].to_list()
                for symbol in symbols:
                    file_path = os.path.join(data_dir, symbol + ".csv")
                    if not os.path.isfile(file_path):
                        continue
                    self.spark.sparkContext.addFile(file_path)
                    stocks_raw = self.spark.read.csv('file://{}'.format(file_path), header=True, inferSchema=True)
                    stocks_raw = stocks_raw.withColumn("symbol",F.lit(symbol))

                    self.DButils.save_to_db(stocks_raw, table_name)
            logging.info("Created Raw table name %s", table_name)
        except Exception as ex:
            logging.error("Error in write_raw %s", ex)
            raise ex

if __name__ == "__main__":
    extract = Extract()
    extract.extract_from_source()
    extract.store_raw_in_db()