import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import year, month, dayofmonth
from sparktasks.utils.DBUtils import DButils
from sparktasks.utils.config import Config
from sparktasks.utils.utils import UdfUtils
import pyspark.sql.functions as F
from pyspark.sql.window import Window as W
import dateutil.relativedelta
from datetime import datetime, timedelta


class AnalyticsEtl:
    sector_type = 'STOCKS_MONTHLY_ANALYTICS'
    logger = logging.getLogger(' sparktasks.stock.AnalyticsEtl')

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder\
                                 .appName('StocksAnalyticsEtl') \
                                 .getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)
        stcks_dim = self.config.stocks_dim
        self.stocks_dim_df = self.DButils.load_from_db(self.spark, stcks_dim)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        self.logger.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' 
        ORDER BY execution_date desc) foo""" \
            .format(self.config.metadata, self.sector_type)

    def get_table_query(self, ticker):
        date_time = self.get_last_exec_date(ticker)
        if date_time:
            now = datetime.now()
            first = now.replace(day=24)
            new_date = first + dateutil.relativedelta.relativedelta(months=-2)
            date_time = new_date.strftime("%Y-%m-%d")
            return """(SELECT * FROM {} WHERE  symbol= '{}' AND Date >= '{}' ) foo""" \
                .format(self.config.stocks_raw, ticker, date_time)
        now = datetime.now()
        first = now.replace(day=1,month=1,year=2019)
        date_time = first.strftime("%Y-%m-%d")
        return """(SELECT * FROM {} WHERE  symbol= '{}' AND Date >= '{}' ) foo"""\
            .format(self.config.stocks_raw, ticker,date_time)

    def get_last_exec_date(self, ticker):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == ticker).first()
        if row:
            record_date = row[4]
            return record_date.strftime("%Y-%m-%d")
        return row

    def transform_load_analytics_tables(self):
        try:
            self.stocks_dim_df.createOrReplaceTempView(self.config.stocks_dim)
            sp_df = self.spark.sql("SELECT id, symbol, category_name FROM {}"
                                   .format(self.config.stocks_dim)).toPandas()
            for index, row in sp_df.iterrows():
                self.load_analytics_data(row['symbol'], row['category_name'])
        except Exception as ex:
            logging.error("Error generate aggregate tables %s", ex)
            raise ex

    def load_analytics_data(self, ticker, sector):
        try:
            if datetime.now().day != 1:
                return

            stocks_raw = self.DButils.load_from_db(self.spark, self.get_table_query(ticker))
            if stocks_raw.count() == 0:
                return
            date_udf = udf(lambda d: UdfUtils.convert_to_date_world(d), DateType())
            stocks_raw = stocks_raw.fillna(0)
            stocks_fact = stocks_raw.withColumnRenamed("close", "closing_price") \
                .withColumn("stock_date", date_udf(stocks_raw.Date)) \
                .withColumn("symbol", F.lit(ticker)) \
                .withColumn("category_name", F.lit(sector))
            stocks_fact_df = stocks_fact.withColumn("year", year(stocks_fact.stock_date)) \
                .withColumn("month", month(stocks_fact.stock_date)) \
                .withColumn("day", dayofmonth(stocks_fact.stock_date))

            windowspec = W.partitionBy(stocks_fact_df['symbol']) \
                .orderBy(stocks_fact_df['stock_date'])

            daily_return = \
                ((stocks_fact_df['closing_price'] / (F.lag(stocks_fact_df['closing_price'])
                                                     .over(windowspec))) - 1) * 100
            stocks_fact_df = stocks_fact_df.select(stocks_fact_df['symbol'], stocks_fact_df['category_name'],
                                                   stocks_fact_df['stock_date'], stocks_fact_df['year'],
                                                   stocks_fact_df['month'], daily_return.alias('daily_return_rate'),
                                                   stocks_fact_df['closing_price'])

            stocks_fact_df.createOrReplaceTempView("stocks")
            stocks_fact_df = self.spark.sql("SELECT "
                                            "symbol,category_name,stock_date,year,month,"
                                            "daily_return_rate,closing_price,last_value(closing_price) "
                                            "OVER (partition BY  symbol,year,month "
                                            "ORDER BY symbol,year, month) AS monthly_close "
                                            "FROM stocks")
            Window_max_date = W.partitionBy(stocks_fact_df['symbol']) \
                .orderBy(stocks_fact_df['symbol'], stocks_fact_df['year'], stocks_fact_df['month'])
            stock_monthly = stocks_fact_df.groupBy('symbol', 'year', 'month') \
                .agg(F.avg('monthly_close').alias('monthly_close'),
                     F.avg('closing_price').alias('monthly_avg'))\
                .sort('symbol', 'year', 'month')
            monthly_return = \
                ((stock_monthly['monthly_close'] / (F.lag(stock_monthly['monthly_close'])
                                                    .over(Window_max_date))) - 1) * 100

            stock_monthly = stock_monthly.select('symbol', 'year', 'month', 'monthly_close','monthly_avg',
                                                 monthly_return.alias('monthly_return_rate'))
            stock_monthly = stock_monthly.filter(stock_monthly.monthly_return_rate.isNotNull())
            stock_monthly = stock_monthly.withColumnRenamed('symbol', 'symbol_monthly') \
                .withColumnRenamed('year', 'year_monthly') \
                .withColumnRenamed('month', 'monthly')
            stocks_monthly_df = stocks_fact_df.join(stock_monthly,
                                                    (stocks_fact_df.symbol == stock_monthly.symbol_monthly) &
                                                    (stocks_fact_df.year == stock_monthly.year_monthly) &
                                                    (stocks_fact_df.month == stock_monthly.monthly))
            if self.get_last_exec_date(ticker):
                stocks_monthly_df = stocks_monthly_df.filter(stocks_monthly_df.stock_date > self.get_last_exec_date(ticker))
            stocks_monthly_df = stocks_monthly_df\
                .select('symbol', 'category_name',  'year', 'month', 'monthly_return_rate', 'monthly_avg').distinct()
            self.DButils.save_to_db(stocks_monthly_df, self.config.stocks_analytics)
            max_date = datetime.today() - timedelta(days=1)
            self.DButils.insert_update_metadata(ticker, stocks_monthly_df.count(), max_date,
                                                ticker, self.sector_type)
        except Exception as ex:
            self.logger.error("Error generate aggregate tables %s", ex)
            raise ex


if __name__ == "__main__":
    transform_load = AnalyticsEtl()
    transform_load.transform_load_analytics_tables()
