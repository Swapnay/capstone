import os
from pyspark.sql import SparkSession
import pandas as pd
from sparktasks.utils.DBUtils import DButils
from sparktasks.utils.config import Config
import datetime
import logging
from pandas_datareader import data
import pyspark.sql.functions as F
import time


class Extract:
    logger = logging.getLogger('sparktasks.housing.Extract')
    sector_type = 'STOCKS'

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('Extract') \
            .config("spark.ui.port", "4070") \
            .getOrCreate()
        self.config = Config()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())

        logging.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}'  ORDER BY execution_date desc) foo""" \
            .format(self.config.metadata, self.sector_type)

    def get_last_record_date(self, symbol):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == symbol).first()
        if row:
            start_date = row[4] + datetime.timedelta(days=1)
        else:
            start_date = self.config.stocks_start_date
        return start_date

    # saves in local folder.Gets previous run from metadata and discards
    # old data.
    def extract_from_source(self):
        try:
            logging.info("Data Extract in progress from %s", self.config.stocks_sp)
            data_dir = self.config.data_dir
            stocks_data = pd.read_csv(self.config.stocks_sp)
            symbols = stocks_data["Symbol"].to_list()

            for symbol in symbols:
                end_date = datetime.datetime.now()
                start_date = self.get_last_record_date(symbol)
                try:
                    panel_data = data.DataReader(symbol, 'yahoo', start_date, end_date)
                    new_path = os.path.join(self.config.data_dir, symbol + ".csv")
                    panel_data.to_csv(new_path)
                except Exception as ex:
                    print(ex)
            full_path = os.path.join(data_dir,"STOCKS_SP_500.csv")
            stocks_data.to_csv(full_path, index=False)
            logging.info("Data Extracted to %s", full_path)
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex

    # Stores CSV in landing database as raw tables
    def store_raw_in_db(self):
        self.write_raw()

    def write_raw(self):
        try:
            start_time = time.time()
            table_name = self.config.stocks_raw
            data_dir = self.config.data_dir
            full_path = os.path.join(data_dir,"STOCKS_SP_500.csv")
            stocks_data = pd.read_csv(full_path)
            symbols = stocks_data["Symbol"].to_list()
            for symbol in symbols:
                file_path = os.path.join(data_dir, symbol + ".csv")
                if not os.path.isfile(file_path):
                    continue
                self.spark.sparkContext.addFile(file_path)
                stocks_raw = self.spark.read.csv('file://{}'.format(file_path), header=True, inferSchema=True)
                start_date = self.get_last_record_date(symbol)
                if type(start_date) != str :
                    start_date = start_date.strftime("%Y-%m-%d")
                else:
                    print("Date is a string {} {}".format(symbol,start_date))
                stocks_df = stocks_raw.filter(stocks_raw.Date >=start_date)
                if stocks_df.count() == 0:
                    print("no more to save {}".format(symbol))
                    continue
                stocks_raw = stocks_df.withColumn("symbol", F.lit(symbol))
                self.DButils.save_to_db(stocks_raw, table_name)
                max_date = stocks_raw.select(F.max("Date")).first()[0]
                record_count = stocks_raw.count()
                self.DButils.insert_update_metadata(symbol, record_count, max_date, symbol, self.sector_type)

            end_time = time.time()
            print("it took this long to run load_stock_data: {}".format(end_time - start_time))
            logging.info("it took this long to run load_stock_data: {}".format(end_time - start_time))

            logging.info("Created Raw table name %s", table_name)
        except Exception as ex:
            logging.error("Error in write_raw %s", ex)
            raise ex


if __name__ == "__main__":
    extract = Extract()
    extract.extract_from_source()
    extract.store_raw_in_db()
