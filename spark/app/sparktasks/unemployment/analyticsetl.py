from pyspark.sql import SparkSession
import logging
from sparktasks.utils.DBUtils import DButils
from sparktasks.utils.config import Config
from datetime import datetime, timedelta
import pyspark.sql.functions as F

class AnalyticsEtl:

    sector_type = 'UNEMPLOYMENT_ANALYTICS'


    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder.appName('UnemploymentAnalyticsEtl') \
            .getOrCreate()
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        self.state_df = self.DButils.load_from_db(self.spark, self.config.state_dim)
        logging.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' ORDER BY execution_date desc) foo""" \
            .format(self.config.metadata, self.sector_type )

    def get_table_query(self, name, sector_sub_type):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == sector_sub_type).first()
        record_date = datetime.now()
        date_time = record_date.replace(year=2019,month=1,day=1)
        date_time = date_time.strftime("%Y-%m-%d")
        if row:
            record_date = row[4]
            record_date = record_date + timedelta(days=1)
            # record_date =record_date.replace(day=1)
            date_time = record_date.strftime("%Y-%m-%d")
            return """(SELECT * FROM {} WHERE  cast(submission_date as Date) > '{}') foo""".format(name, date_time)
        return """(SELECT * FROM {} WHERE  cast(submission_date as Date)>= '{}') foo""".format(name, date_time)

    def transform_load_analytics_tables(self):
        for name in dict(self.config.employment).keys():
            try:
                if name.endswith("25") | name.endswith("50"):
                    continue
                fact_table = self.config.get_config('FACTS', name)
                unemployment_df = self.DButils.load_from_db(self.spark, self.get_table_query(fact_table,name))
                if unemployment_df.count() == 0:
                    continue
                unemployment_df.createOrReplaceTempView(name.lower())
                unemployment_df = self.convert_to_target_df(name)
                unemployment_df = unemployment_df.select('variable_type', 'variable_name', 'submission_date', \
                                                          'year', 'month', 'unemployed_rate' )

                self.DButils.save_to_db(unemployment_df, self.config.unemployment_analytics)
                max_date = unemployment_df.agg(F.max('submission_date')).first()[0]
                self.DButils.insert_update_metadata(name, unemployment_df.count(), max_date,
                                                    name, self.sector_type)
            except Exception as ex:
                logging.error("Error extracting data %s", ex)
                raise ex

    def convert_to_target_df(self, name):
        if name.lower() == 'unemployment_by_race':
            return self.get_data_by_race( name)
        elif name.lower() == 'unemployment_by_education_level':
            return
            #return self.get_data_by_education(name)
        elif name.lower() == 'unemployment_by_industry':
            return self.get_data_by_industry( name)
        else:
            return self.get_data_by_state(name)

    def get_data_by_race(self,  name):
        query = ("SELECT 'race' as variable_type,race_type as variable_name,"\
                        "unemployed_rate, year(submission_date) as year,"\
                        "month(submission_date) as month, Date(submission_date) AS submission_date "\
                "FROM {} ORDER BY variable_name,year,month").format(name.lower())
        return self.spark.sql(query)

    def get_data_by_education(self,  name):
        query = ("SELECT 'education' as variable_type,education as variable_name," \
                         "unemployed_rate, year(submission_date) as year," \
                         "month(submission_date) as month, submission_date " \
                 "FROM {} ORDER BY variable_name,year,month").format(name.lower())
        return self.spark.sql(query)

    def get_data_by_industry(self,  name):
        query = ("SELECT 'industry' AS variable_type,industry_type as variable_name," \
                         "unemployment_rate AS unemployed_rate, year(submission_date) AS year," \
                         "month(submission_date) AS month, submission_date " \
                 "FROM {} ORDER BY variable_name,year,month").format(name.lower())
        return self.spark.sql(query)

    def get_data_by_state(self,  name):
        query = ("SELECT 'state' AS variable_type,state_id," \
                         "unemployment_rate AS unemployed_rate, year(submission_date) AS year," \
                         "month(submission_date) AS month, submission_date " \
                 "FROM {} ORDER BY state_id,year,month").format(name.lower())
        unemployment_df = self.spark.sql(query)
        return unemployment_df.join(self.state_df, self.state_df.id==unemployment_df.state_id, how='inner')\
                                         .withColumnRenamed('code','variable_name')


if __name__ == "__main__":
    transform_load = AnalyticsEtl()
    transform_load.transform_load_analytics_tables()