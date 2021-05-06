import logging
import os
from sparktasks.utils.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import year, month, dayofmonth
from sparktasks.utils.utils import UdfUtils
from sparktasks.utils.DBUtils import DButils
import pyspark.sql.functions as F
import time
from pyspark.sql.functions import broadcast
from datetime import datetime


class TransformLoad:
    logger = logging.getLogger('sparktasks.covid.TransformLoad')
    sector_type = 'STOCKS'

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder\
                                 .appName('StocksTransformLoadc')\
                                 .getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        date_table = self.config.date_dim
        self.stocks_dim_df = self.DButils.load_from_db(self.spark, self.config.stocks_dim)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        self.date_dim_df = self.DButils.load_from_db(self.spark, date_table)
        logging.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_sub_type = '{}' and  sector_type = '{}' ORDER BY execution_date desc) foo""".format(self.config.metadata,
                                                                                                         self.sector_type,self.sector_type)

    def get_date_dim_query(self):
        return """(SELECT * FROM {} WHERE  year > {}) foo""".format(self.config.date_dim, '2008')

    def get_stocks_dim_query(self):
        return """(SELECT * FROM {} ) foo""".format(self.config.stocks_dim)

    def get_table_query(self):
        start_date=self.get_start_date()
        return """(SELECT * FROM {} WHERE  Date > '{}') foo""".format(self.config.stocks_raw, start_date)

    def get_start_date(self):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == self.sector_type).first()
        if row:
            record_date = row[4]
            date_time = record_date.strftime("%Y-%m-%d")
        else:
            date_time = self.config.stocks_start_date
        return date_time

    def transform_load_data(self):
        data_dir = self.config.data_dir
        sp_stocks = os.path.join(data_dir,"STOCKS_SP_500.csv")
        # file_name = os.path.basename(sp_stocks)
        if os.path.isfile(sp_stocks) & self.stocks_dim_df.count() == 0:
            self.spark.sparkContext.addFile(sp_stocks)
            stocks_sp_dim = self.spark.read.csv('file://{}'.format(sp_stocks), header=True,
                                                inferSchema=True)
            stocks_sp_dim = stocks_sp_dim.fillna(0)
            stocks_sp_dim = stocks_sp_dim.withColumnRenamed("Symbol", "symbol") \
                .withColumnRenamed("Name", "name") \
                .withColumnRenamed("Sector", "category_name")
            self.DButils.save_to_db(stocks_sp_dim, self.config.stocks_dim)
            self.stocks_dim_df = self.DButils.load_from_db(self.spark, self.config.stocks_dim)
        self.stocks_dim_df.createOrReplaceTempView(self.config.stocks_dim)
        self.load_stock_data()

    def load_stock_data(self):
        start_time = time.time()
        stocks_fact = self.DButils.load_from_db(self.spark, self.get_table_query()) #'Date', self.get_start_date(),datetime.now().date())
        if stocks_fact.count() == 0:
            return
        print("partitions {}".format(stocks_fact.rdd.getNumPartitions()))
        date_udf = udf(lambda d: UdfUtils.convert_to_date_stocks(d), DateType())
        stocks_fact = stocks_fact.fillna(0)
        stocks_fact = stocks_fact.withColumnRenamed("High", "high") \
            .withColumnRenamed("Low", "low") \
            .withColumnRenamed("open", "open_price") \
            .withColumnRenamed("close", "closing_price") \
            .withColumnRenamed("Volume", "volume") \
            .withColumn("stock_date", date_udf(stocks_fact.Date))
        stocks_fact_df = stocks_fact.join(self.stocks_dim_df,self.stocks_dim_df.symbol==stocks_fact.symbol, how="inner")\
                                    .withColumnRenamed('id','stock_id')
        stocks_fact_df = stocks_fact_df.withColumn("year", year(stocks_fact_df.stock_date)) \
            .withColumn("month", month(stocks_fact_df.stock_date)) \
            .withColumn("day", dayofmonth(stocks_fact_df.stock_date))
        stocks_fact_df = stocks_fact_df.join(self.date_dim_df, (stocks_fact_df.day == self.date_dim_df.day) &
                                             (stocks_fact_df.month == self.date_dim_df.month) &
                                             (stocks_fact_df.year == self.date_dim_df.year), how="inner")
        stocks_fact_df = stocks_fact_df.withColumnRenamed("id", "date_id")
        stocks_fact_df = stocks_fact_df.select("stock_id", "date_id", "stock_date", "open_price", "closing_price",
                                               "high", "low", "volume")

        print("partitions {}".format(stocks_fact.rdd.getNumPartitions()))
        stocks_fact_df.explain(True)
        self.DButils.save_to_db(stocks_fact_df, self.config.stocks_fact)
        max_date = stocks_fact_df.select(F.max("stock_date")).first()[0]
        record_count = stocks_fact_df.count()
        self.DButils.insert_update_metadata(self.sector_type, record_count, max_date, self.sector_type, self.sector_type)
        end_time = time.time()
        print("it took this long to run load_stock_data: {}".format(end_time - start_time))
        logging.info("it took this long to run load_stock_data: {}".format(end_time - start_time))


if __name__ == "__main__":
    transform_load = TransformLoad()
    transform_load.transform_load_data()
