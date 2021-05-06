import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import year, month, dayofmonth
from sparktasks.utils.DBUtils import DButils
from sparktasks.utils.config import Config
from sparktasks.utils.utils import UdfUtils
import pyspark.sql.functions as F
from datetime import datetime, timedelta
import time


class AnalyticsEtl:
    sector_type_world = 'COVID_WORLD_ANALYTICS'
    sector_type_usa = 'COVID_USA_ANALYTICS'
    sector_type = 'COVID'

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder.appName('CovidAnalyticsEtl') \
            .getOrCreate()
        self.spark.conf.set("spark.sql.shuffle.partitions", 20)

        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        logging.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' ORDER BY execution_date desc) foo""" \
            .format(self.config.metadata, self.sector_type)

    def get_table_query(self, name, sector_sub_type, date_field):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == sector_sub_type).first()
        if row:
            record_date = row[4]
            date_time = record_date.strftime("%Y-%m-%d")
            return """(SELECT * FROM {} WHERE  STR_TO_DATE({},'%Y-%m-%d') > '{}') foo""".format(name, date_field, date_time)
        return name

    def transform_load_analytics_tables(self):
        if datetime.now().day != 1:
            return
        self.transform_load_usa()
        self.transform_load_world()

    def transform_load_usa(self):
        try:
            logging.info("Transform and load usa data")
            covid_usa_raw = self.config.covid_usa_raw
            covid_usa_df = self.DButils.load_from_db(self.spark, self.get_table_query(covid_usa_raw, self.sector_type_usa, 'submission_date'))
            if covid_usa_df.count() == 0:
                return
            start_time = time.time()
            covid_usa_df.createOrReplaceTempView(covid_usa_raw)
            covid_usa_df = self.spark.sql("SELECT state,submission_date, new_case,tot_cases,tot_death,new_death "
                                          "FROM {}".format(covid_usa_raw))
            date_udf = udf(lambda d: UdfUtils.convert_to_date(d), DateType())
            covid_usa_df = covid_usa_df.withColumn("submission_date", date_udf(covid_usa_df.submission_date))
            covid_usa_df = covid_usa_df.withColumn("year", year(covid_usa_df.submission_date)) \
                .withColumn("month", month(covid_usa_df.submission_date)) \
                .withColumn("day", dayofmonth(covid_usa_df.submission_date))
            today = datetime.now()
            today = today.replace(day=1)
            covid_usa_df = covid_usa_df.filter(covid_usa_df.submission_date < F.lit(today.date()))
            covid_usa_df = covid_usa_df.groupBy(covid_usa_df['state'], covid_usa_df["year"],
                                                covid_usa_df["month"]) \
                .agg(F.sum(covid_usa_df["new_case"]).alias('monthly_new_cases'),
                     F.max(covid_usa_df["tot_cases"]).alias('total_cases'),
                     F.avg(covid_usa_df["new_case"]).alias('avg_new_cases'),
                     F.avg(covid_usa_df["new_death"]).alias('avg_new_deaths'),
                     F.max(covid_usa_df["tot_death"]).alias('total_deaths'),
                     F.sum(covid_usa_df["new_death"]).alias('monthly_new_deaths')
                     )
            covid_usa_df_monthly = covid_usa_df.select('state', 'year', 'month', 'monthly_new_cases',
                                                       'avg_new_cases', 'avg_new_deaths', 'total_cases',
                                                       'total_deaths', 'monthly_new_deaths')
            self.DButils.save_to_db(covid_usa_df_monthly, self.config.covid_usa_analytics)
            max_date = today.date() - timedelta(days=1)
            self.DButils.insert_update_metadata(self.sector_type_usa, covid_usa_df_monthly.count(), max_date,
                                                self.sector_type_usa, self.sector_type)
            end_time = time.time()
            print("It took this long to run transform_load_usa: {}".format(end_time - start_time))
            logging.info("It took this long to run transform_load_usa: {}".format(end_time - start_time))
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex

    def transform_load_world(self):
        try:
            logging.info("Transform and load world data")
            covid_world_raw = self.config.covid_world_raw
            covid_world_df = self.DButils.load_from_db(self.spark, self.get_table_query(covid_world_raw, self.sector_type_world, 'date'))
            if covid_world_df.count() == 0:
                return
            start_time = time.time()
            covid_world_df.createOrReplaceTempView(covid_world_raw)
            covid_world_df = self.spark.sql("SELECT iso_code as country,location as country_name, date as submission_date,new_deaths, "
                                            "new_cases,total_cases,total_deaths,total_tests,new_tests,population "
                                            "FROM {}".format(covid_world_raw))
            date_udf = udf(lambda d: UdfUtils.convert_to_date_world(d), DateType())
            covid_world_df = covid_world_df.withColumn("submission_date", date_udf(covid_world_df.submission_date))
            covid_world_dt_df = covid_world_df.withColumn("year", year(covid_world_df.submission_date)) \
                .withColumn("month", month(covid_world_df.submission_date)) \
                .withColumn("day", dayofmonth(covid_world_df.submission_date))
            today = datetime.now()
            today = today.replace(day=1)
            covid_world_dt_df = covid_world_dt_df.fillna(0)
            covid_world_dt_df = covid_world_dt_df.filter(covid_world_dt_df.submission_date < F.lit(today.date()))
            covid_world_dt_df = covid_world_dt_df.groupBy(covid_world_dt_df['country'], covid_world_dt_df['country_name'], covid_world_dt_df["year"],
                                                          covid_world_dt_df["month"], covid_world_dt_df["population"]) \
                .agg(F.sum(covid_world_dt_df["new_cases"]).alias('monthly_new_cases'),
                     F.sum(covid_world_dt_df["new_tests"]).alias('monthly_tests'),
                     F.sum(covid_world_dt_df["new_deaths"]).alias('monthly_new_deaths'),
                     F.avg(covid_world_dt_df["new_cases"]).alias('avg_new_cases'),
                     F.avg(covid_world_dt_df["new_deaths"]).alias('avg_new_deaths'),
                     F.max(covid_world_dt_df["total_cases"]).alias('total_cases'),
                     F.max(covid_world_dt_df["total_deaths"]).alias('total_deaths'),
                     F.max(covid_world_dt_df["total_tests"]).alias('total_tests')
                     )
            covid_world_monthly = covid_world_dt_df.select('country', 'country_name', 'month', 'year', 'avg_new_cases', 'monthly_new_cases', 'avg_new_deaths',
                                                           'monthly_new_deaths', 'monthly_tests', 'total_cases', 'total_deaths', 'population')
            self.DButils.save_to_db(covid_world_monthly, self.config.covid_world_analytics)
            max_date = today.date() - timedelta(days=1)
            self.DButils.insert_update_metadata(self.sector_type_world, covid_world_monthly.count(), max_date,
                                                self.sector_type_world, self.sector_type)
            end_time = time.time()
            print("It took this long to run transform_load_world: {}".format(end_time - start_time))
            logging.info("It took this long to run transform_load_world: {}".format(end_time - start_time))
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex


if __name__ == "__main__":
    transform_load = AnalyticsEtl()
    transform_load.transform_load_analytics_tables()
