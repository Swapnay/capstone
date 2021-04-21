import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import StringType,DateType
from spark.app.sparktasks.utils.config import Config
from spark.app.sparktasks.utils.DBUtils import DButils
from spark.app.sparktasks.utils.utils import UdfUtils
import pyspark.sql.functions as F
from datetime import datetime


class TransformLoad:
    logger = logging.getLogger('sparktasks.employment.TransformLoad')
    sector_type ='UNEMPLOYMENT'

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder.appName('EmploymentTransformLoad').config("spark.ui.port", "4065").getOrCreate()
        self.spark.conf.set("spark.sql.crossJoin.enabled", "True")
        self.date_dim_df = self.DButils.load_from_db(self.spark, self.config.get_config('DIMS', 'HOUSING_DATE_DIM'))
        self.state_df = self.DButils.load_from_db(self.spark, self.config.state_dim)
        self.unemployment_series = self.DButils.load_from_db(self.spark, self.config.unemployment_series_dim)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' ORDER BY execution_date desc) foo""".format(self.config.metadata, self.sector_type)

    def get_table_query(self, name,raw):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == name).first()
        if row:
            record_date = row[4]
            #date_time = record_date.strftime("%Y-%m-%d")
            year = record_date.year
            month=record_date.month
            if month == 12:
                month=1
                year=year+1
            if month<10:
                month_str = 'M0'+str(month+1)
            else:
                month_str = 'M'+str(month+1)
            return """(SELECT * FROM {} WHERE  Results_series_data_year>= {} AND 
             Results_series_data_period >= '{}') foo""".format(raw, year, month_str)
            #convert(SUBSTRING_INDEX(Results_series_data_period,'M',-1),UNSIGNED INTEGER) >= {}) foo"""\

        return raw

    def exec_query(self, unemployment_df, name):
        if name.lower() == 'unemployment_by_race':
            return self.get_data_by_race(unemployment_df, name)
        elif name.lower() == 'unemployment_by_education_level':
            return self.get_data_by_education(unemployment_df, name)
        elif name.lower() == 'unemployment_by_industry':
            return self.get_data_by_industry(unemployment_df, name)
        else:
            return self.get_data_by_state(unemployment_df, name)

    def get_data_by_state(self, unemployment_df, name):
        try:
            logging.info(" load data %s", name)
            unemployment_df.createGlobalTempView(name)
            view_name = "{}.{}".format("global_temp", name)
            self.spark.read.table(view_name)

            query = "SELECT id as date_id ,area_text AS state,Results_series_data_value AS {}, submission_date " \
                    "FROM {} where area_type_code ={}  "
            union1 = query.format('unemployment_rate', view_name, "\'A\'")
            total_df = self.spark.sql(union1)
            total_df = total_df.join(self.state_df, (total_df.state == self.state_df.name))
            total_df = total_df.withColumnRenamed('id', 'state_id')
            total_df = total_df.filter(total_df.unemployment_rate!='-')
            return total_df.select('date_id', 'state_id', 'unemployment_rate','submission_date')
        except Exception as ex:
            logging.error("Error load data %s", name)
            logging.error("Error load data %s", ex)
            raise ex

    def get_data_by_industry(self, unemployment_df, name):
        try:
            logging.info(" load data %s", name)
            unemployment_df.createGlobalTempView(name)
            view_name = "{}.{}".format("global_temp", name)
            self.spark.read.table(view_name)

            query = "SELECT id ,area_text AS industry_type,Results_series_data_value AS {}{} " \
                    "FROM {} where area_type_code =\'I\' AND stat_type = {} "
            union1 = query.format('unemployment',', submission_date', view_name, "\'U\'")
            total_df = self.spark.sql(union1)
            union2 = query.format('unemployment_rate','', view_name, "\'R\'")
            rate_df = self.spark.sql(union2)

            total_df = total_df.withColumnRenamed('id', 'date_id') \
                .withColumnRenamed('industry_type', 'industry_type1')
            total_df = total_df.join(rate_df, (total_df.date_id == rate_df.id)
                                     & (total_df.industry_type1 == rate_df.industry_type))
            total_df = total_df.filter(total_df.unemployment_rate!='-')
            return total_df.select('date_id', 'industry_type', 'unemployment', 'unemployment_rate', 'submission_date')
        except Exception as ex:
            logging.error("Error load data %s", name)
            logging.error("Error load data %s", ex)
            raise ex

    def get_data_by_education(self, unemployment_df, name):
        try:
            logging.info(" load data %s", name)
            unemployment_df.createGlobalTempView(name)
            view_name = "{}.{}".format("global_temp", name)
            self.spark.read.table(view_name)
            query = "SELECT id ,area_text AS education,Results_series_data_value AS {}{} " \
                    "FROM {} where area_type_code =\'E\' AND stat_type = {} "

            union2 = query.format('participated',', submission_date', view_name, "\'P\'")
            participated_df = self.spark.sql(union2)
            query3 = query.format('participated_rate','', view_name, "\'PR\'")
            participated_rate_df = self.spark.sql(query3)
            query4 = query.format('unemployed','', view_name, "\'U\'")
            unemployment_df = self.spark.sql(query4)
            query5 = query.format('unemployed_rate','', view_name, "\'UR\'")
            unemployment_rate_df = self.spark.sql(query5)
            total_df = participated_df.withColumnRenamed('id', 'date_id') \
                .withColumnRenamed('education', 'education1')
            total_df = total_df.join(participated_rate_df, (total_df.date_id == participated_rate_df.id) &
                                     (total_df.education1 == participated_rate_df.education))
            total_df = total_df.select('date_id', 'education', 'participated', 'participated_rate', 'submission_date')
            total_df = total_df.withColumnRenamed('education', 'education1')
            total_df = total_df.join(unemployment_df, (unemployment_df.id == total_df.date_id) &
                                     (unemployment_df.education == total_df.education1))
            unemp_df = total_df.select('date_id', 'education', 'participated', 'participated_rate', 'unemployed', 'submission_date')
            final_unemp_df = unemp_df.withColumnRenamed('education', 'education1')
            final_unemp_df = final_unemp_df.join(unemployment_rate_df, (unemployment_rate_df.id == final_unemp_df.date_id) &
                                                 (unemployment_rate_df.education == final_unemp_df.education1))
            final_unemp_df = final_unemp_df.filter(final_unemp_df.unemployed_rate!='-')
            return final_unemp_df.select('date_id', 'education', 'participated', 'participated_rate',
                                         'unemployed', 'unemployed_rate', 'submission_date')
        except Exception as ex:
            logging.error("Error load data %s", name)
            logging.error("Error load data %s", ex)
            raise ex

    def get_data_by_race(self, unemployment_df, name):
        try:
            logging.info(" load data %s", name)
            unemployment_df.createGlobalTempView(name)
            view_name = "{}.{}".format("global_temp", name)
            self.spark.read.table(view_name)
            query = "SELECT id ,area_text AS race_type,Results_series_data_value AS {}{} " \
                    "FROM {} where area_type_code =\'R\' AND stat_type = {} "
            union1 = query.format('civilian_noninstitutional',', submission_date', view_name, "\'T\'")
            total_df = self.spark.sql(union1)
            union2 = query.format('participated','', view_name, "\'P\'")
            participated_df = self.spark.sql(union2)
            query3 = query.format('participated_rate','', view_name, "\'PR\'")
            participated_rate_df = self.spark.sql(query3)
            query4 = query.format('unemployed','', view_name, "\'U\'")
            unemployment_df = self.spark.sql(query4)
            query5 = query.format('unemployed_rate','', view_name, "\'UR\'")
            unemployment_rate_df = self.spark.sql(query5)
            total_df = total_df.withColumnRenamed('id', 'date_id') \
                .withColumnRenamed('race_type', 'area_text1')
            total_df = total_df.join(participated_df, (total_df.date_id == participated_df.id)
                                     & (total_df.area_text1 == participated_df.race_type))

            total_df = total_df.select('date_id', 'race_type', 'civilian_noninstitutional', 'participated', 'submission_date')
            total_df = total_df.withColumnRenamed('race_type', 'area_text1')
            total_df = total_df.join(participated_rate_df, (participated_rate_df.id == total_df.date_id) &
                                     (participated_rate_df.race_type == total_df.area_text1))
            total_df = total_df.select('date_id', 'race_type', 'civilian_noninstitutional', 'participated',
                                       'participated_rate','submission_date')

            final_df = total_df.withColumnRenamed('race_type', 'area_text1')
            final_df = final_df.join(unemployment_df, (unemployment_df.id == total_df.date_id)
                                     & (final_df.area_text1 == unemployment_df.race_type))
            unemp_df = final_df.select('date_id', 'race_type', 'civilian_noninstitutional', 'participated',
                                       'participated_rate', 'unemployed','submission_date')
            final_unemp_df = unemp_df.withColumnRenamed('race_type', 'area_text1')
            final_unemp_df = final_unemp_df.join(unemployment_rate_df, (unemployment_rate_df.id == unemp_df.date_id)
                                                 & (final_unemp_df.area_text1 == unemployment_rate_df.race_type))
            final_unemp_df = final_unemp_df.filter(final_unemp_df.unemployed_rate!='-')
            return final_unemp_df.select('date_id', 'race_type', 'civilian_noninstitutional', 'participated',
                                         'participated_rate', 'unemployed', 'unemployed_rate','submission_date')
        except Exception as ex:
            logging.error("Error load data %s", name)
            logging.error("Error load data %s", ex)
            raise ex

    def transform_load_data(self):
        for name in dict(self.config.employment).keys():
            try:
                if name.endswith("25") | name.endswith("50"):
                    continue
                raw = self.config.get_config('RAW', name)
                fact_table = self.config.get_config('FACTS', name)
                unemployment_df = self.DButils.load_from_db(self.spark, self.get_table_query(name,raw))
                if unemployment_df.count() == 0:
                    continue
                split_udf = udf(lambda d: UdfUtils.get_month(d), StringType())
                ue_df = unemployment_df.withColumn("umonth", split_udf(unemployment_df.Results_series_data_period))
                ue_df = ue_df.join(self.unemployment_series, (unemployment_df.Results_series_seriesID
                                                              == self.unemployment_series.area_code))
                ue_df = ue_df.withColumnRenamed('id', 'series_id')
                unemployment_df = ue_df.join(self.date_dim_df, (self.date_dim_df.month == ue_df.umonth)
                                             & (self.date_dim_df.year == ue_df.Results_series_data_year))
                date_udf =udf(lambda d, y: UdfUtils.get_date(d,y), DateType())
                unemployment_df =unemployment_df.withColumn('submission_date',date_udf(unemployment_df.umonth,unemployment_df.year))
                unemp_df = self.exec_query(unemployment_df, name)

                #unemp_df = unemp_df.withColumn('submission_date', F.lit(datetime.now())).orderBy('date_id')

                max_date = unemp_df.agg(F.max('date_id')).first()[0]
                self.DButils.save_to_db(unemp_df, self.config.get_config('FACTS', name))
                df_date = self.date_dim_df.filter(self.date_dim_df.id == max_date)
                month = df_date.select("month").first()[0]
                year = df_date.select("year").first()[0]
                record_count = unemp_df.count()
                date_time_str = '{}-{}-{}'.format(year, month, '2')
                date_time_obj = datetime.strptime(date_time_str, "%Y-%m-%d")
                self.DButils.insert_update_metadata(name, record_count, date_time_obj, name, self.sector_type)
            except Exception as ex:
                logging.error("Error load data %s", name)
                logging.error("Error load data %s", ex)
                raise ex


if __name__ == "__main__":
    transform_load = TransformLoad()
    transform_load.transform_load_data()
