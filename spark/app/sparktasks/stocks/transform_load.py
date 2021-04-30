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


class TransformLoad:
    logger = logging.getLogger('sparktasks.covid.TransformLoad')
    sector_type = 'STOCKS'

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder.appName('StocksTransformLoadc').getOrCreate()
        date_table = self.config.date_dim
        self.date_dim_df = self.DButils.load_from_db(self.spark, date_table)
        self.date_dim_df.createGlobalTempView(date_table)
        stcks_dim = self.config.stocks_dim
        self.stocks_dim_df = self.DButils.load_from_db(self.spark, stcks_dim)
        # self.stocks_dim_df.createGlobalTempView(self.stocks_dim_df)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())

        logging.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' ORDER BY execution_date desc) foo""".format(self.config.metadata,
                                                                                                         self.sector_type)

    def get_table_query(self, ticker):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == ticker).first()
        if row:
            record_date = row[4]
            date_time = record_date.strftime("%Y-%m-%d")
            return """(SELECT * FROM {} WHERE  symbol= '{}' AND Date > '{}') foo""".format(self.config.stocks_raw, ticker, date_time)
        return """(SELECT * FROM {} WHERE  symbol= '{}') foo""".format(self.config.stocks_raw, ticker)


    def transform_load_data(self):
        for name, value in dict(self.config.stocks).items():
            sp_stocks = os.path.join(self.config.data_dir, name + ".csv")
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

        sp_df = self.spark.sql("SELECT id, symbol FROM {}".format(self.config.stocks_dim)).toPandas()
        for index, row in sp_df.iterrows():
            self.load_stock_data(row['id'], row['symbol'])

    def load_stock_data(self, id, ticker):
        data_dir = self.config.data_dir
        file_path = os.path.join(data_dir, ticker + ".csv")
        if os.path.isfile(file_path):
            self.spark.sparkContext.addFile(file_path)
            stocks_fact = self.DButils.load_from_db(self.spark, self.get_table_query(ticker))
            if stocks_fact.count() == 0:
                return
            date_udf = udf(lambda d: UdfUtils.convert_to_date_world(d), DateType())
            stocks_fact = stocks_fact.fillna(0)
            stocks_fact = stocks_fact.withColumnRenamed("High", "high") \
                .withColumnRenamed("Low", "low") \
                .withColumnRenamed("open", "open_price") \
                .withColumnRenamed("close", "closing_price") \
                .withColumnRenamed("Volume", "volume") \
                .withColumn("stock_date", date_udf(stocks_fact.Date)) \
                .withColumn("stock_id", F.lit(id))
            stocks_fact_df = stocks_fact.withColumn("year", year(stocks_fact.stock_date)) \
                .withColumn("month", month(stocks_fact.stock_date)) \
                .withColumn("day", dayofmonth(stocks_fact.stock_date))
            stocks_fact_df = self.date_dim_df.join(stocks_fact_df, (stocks_fact_df.day == self.date_dim_df.day) &
                                                   (stocks_fact_df.month == self.date_dim_df.month) &
                                                   (stocks_fact_df.year == self.date_dim_df.year), how="inner")
            stocks_fact_df = stocks_fact_df.withColumnRenamed("id", "date_id")
            stocks_fact_df = stocks_fact_df.select("stock_id", "date_id", "stock_date", "open_price", "closing_price",
                                                   "high", "low", "volume")

            self.DButils.save_to_db(stocks_fact_df, self.config.stocks_fact)
            max_date = stocks_fact_df.select(F.max("stock_date")).first()[0]
            record_count = stocks_fact_df.count()
            self.DButils.insert_update_metadata(ticker, record_count, max_date, ticker,self.sector_type)


if __name__ == "__main__":
    transform_load = TransformLoad()
    transform_load.transform_load_data()
