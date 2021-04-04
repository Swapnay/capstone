import logging
from pyspark.sql import SparkSession
from pyspark.sql.functions import udf
from pyspark.sql.types import DateType
from pyspark.sql.functions import year, month, dayofmonth
import pyspark.sql.functions as f
from spark.app.sparktasks.utils.DBUtils import DButils
from spark.app.sparktasks.utils.config import Config
from spark.app.sparktasks.utils.utils import UdfUtils


class TransformLoad:
    logger = logging.getLogger('sparktasks.covid.TransformLoad')
    sector_type ='COVID'

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('CovidTransformLoad').getOrCreate()
        self.config = Config()
        date_table = self.config.date_dim
        self.date_dim_df = self.DButils.load_from_db(self.spark, date_table)
        self.date_dim_df.createGlobalTempView(date_table)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        state_dim = self.config.state_dim
        self.state_df = self.DButils.load_from_db(self.spark, state_dim)
        self.state_df.createGlobalTempView(state_dim)
        country_dim = self.config.country_details_dim
        self.country_df = self.DButils.load_from_db(self.spark, country_dim)
        self.country_df.createGlobalTempView(country_dim)

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' ORDER BY execution_date desc) foo""".format(self.config.metadata, self.sector_type)

    def transform_load_data(self):
        self.transform_load_usa_data()
        self.transform_load_world_data()

    def get_usa_table_query(self):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == self.config.covid_usa_sector).first()
        if row:
            record_date = row[4]
            date_time = record_date.strftime("%Y-%m-%d")
            return """(SELECT * FROM {} WHERE  STR_TO_DATE(submission_date,'%m/%d/%Y') > '{}') foo""".format(self.config.covid_usa_raw, date_time)
        return self.config.covid_usa_raw

    def get_table_query(self, name):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == self.config.covid_world_sector).first()
        if row:
            record_date = row[4]
            date_time = record_date.strftime("%Y-%m-%d")
            return """(SELECT * FROM {} WHERE  STR_TO_DATE(date,'%Y-%m-%d') > '{}') foo""".format(name, date_time)
        return name

    def transform_load_world_data(self):
        try:
            logging.info("Transform and load world data")
            self.spark.conf.set("spark.sql.execution.arrow.enabled", "False")
            covid_world_raw = self.config.covid_world_raw
            covid_world_df = self.DButils.load_from_db(self.spark, self.get_table_query(covid_world_raw))
            if covid_world_df.count() ==0:
                return
            covid_world_df.createOrReplaceTempView(covid_world_raw)
            country_details_dim = self.config.country_details_dim
            if self.country_df.count() == 0:
                view_name = "{}.{}".format("global_temp", covid_world_raw)
                country_dim_df = self.spark.sql("SELECT iso_code,location, continent,population,population_density,median_age,aged_65_older,"
                                                " aged_70_older,gdp_per_capita,extreme_poverty,cardiovasc_death_rate,diabetes_prevalence,"
                                                "handwashing_facilities,hospital_beds_per_thousand,life_expectancy,human_development_index"
                                                " FROM {}".format(covid_world_raw))
                country_details_df = country_dim_df.withColumnRenamed("location", "country_name")
                self.country_df = country_details_df.select("iso_code", "country_name", 'continent', 'population', 'population_density',
                                                            'median_age', 'aged_65_older', 'aged_70_older', 'gdp_per_capita', 'extreme_poverty',
                                                            'cardiovasc_death_rate', 'diabetes_prevalence', 'handwashing_facilities',
                                                            'hospital_beds_per_thousand', 'life_expectancy', 'human_development_index') \
                    .distinct()

                self.DButils.save_to_db(self.country_df, country_details_dim)
                self.country_df = self.DButils.load_from_db(self.spark, country_details_dim)

            covid_world_df = self.spark.sql("SELECT iso_code as country_code, date as submission_date,new_deaths, new_cases,total_cases,"
                                            "total_deaths,icu_patients,hosp_patients,weekly_icu_admissions,weekly_hosp_admissions,total_tests,"
                                            "new_tests,tests_per_case,positive_rate,tests_units,total_vaccinations,people_vaccinated,"
                                            "people_fully_vaccinated,new_vaccinations,stringency_index "
                                            "FROM {}".format(covid_world_raw))
            date_udf = udf(lambda d: UdfUtils.convert_to_date_world(d), DateType())
            covid_world_df = covid_world_df.withColumn("submission_date", date_udf(covid_world_df.submission_date)) \
                .join(self.country_df, (self.country_df.iso_code == covid_world_df.country_code))
            covid_world_dt_df = covid_world_df.withColumnRenamed("id", "country_id") \
                .withColumn("year", year(covid_world_df.submission_date)) \
                .withColumn("month", month(covid_world_df.submission_date)) \
                .withColumn("day", dayofmonth(covid_world_df.submission_date))
            covid_world = self.date_dim_df.join(covid_world_dt_df, (covid_world_dt_df.day == self.date_dim_df.day) &
                                                (covid_world_dt_df.month == self.date_dim_df.month) &
                                                (covid_world_dt_df.year == self.date_dim_df.year), how="inner")

            covid_world = covid_world.withColumnRenamed("id", "date_id")
            world_data_df = covid_world.select("date_id", "country_id", "submission_date", "new_cases", "total_cases", "total_deaths", "icu_patients",
                                               "hosp_patients", "weekly_icu_admissions", "weekly_hosp_admissions", "total_tests", "new_tests",
                                               "tests_per_case", "positive_rate", "tests_units", "total_vaccinations", "people_vaccinated",
                                               "people_fully_vaccinated", "new_vaccinations", "stringency_index")

            covid_world_fact = self.config.covid_world_fact
            if world_data_df.count() == 0:
                return
            self.DButils.save_to_db(world_data_df, covid_world_fact)
            max_date = world_data_df.select(f.max("submission_date")).first()[0]
            record_count = world_data_df.count()
            self.DButils.insert_update_metadata(self.config.covid_world_sector, record_count, max_date, self.config.covid_world_sector, self.sector_type)
            logging.info("successfully loaded covid world data till %s", max_date)
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex

    def transform_load_usa_data(self):
        # try:
        logging.info("Transform and load usa data")
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "False")
        usa_df1 = self.DButils.load_from_db(self.spark, self.get_usa_table_query())
        if usa_df1.count() ==0:
            return
        date_udf = udf(lambda d: UdfUtils.convert_to_date(d), DateType())
        usa_df = usa_df1.join(self.state_df, usa_df1.state == self.state_df.code)
        new_df = usa_df.withColumn("submit_date", date_udf(usa_df.submission_date)) \
            .withColumnRenamed("id", "state_id") \
            .withColumnRenamed("new_death", "new_deaths") \
            .withColumnRenamed("tot_death", "total_deaths") \
            .withColumnRenamed("new_case", "new_cases") \
            .withColumnRenamed("tot_cases", "total_cases")
        # max_date=new_df.groupby().max('submit_date').collect()[0].asDict()['max(submit_date)']
        max_date = new_df.withColumn("submit_date", f.col("submit_date").cast("timestamp")) \
            .groupBy().agg(f.max("submit_date")).first()[0]
        print(max_date)
        # max_date = new_df.select(f.max(f.col("submit_date"))).first()[0]
        usa_df2 = new_df.withColumn("year", year(new_df.submit_date)) \
            .withColumn("month", month(new_df.submit_date)) \
            .withColumn("day", dayofmonth(new_df.submit_date))
        usa_df2.cache()
        new_df2 = self.date_dim_df.join(usa_df2, (usa_df2.day == self.date_dim_df.day)
                                        & (usa_df2.month == self.date_dim_df.month)
                                        & (usa_df2.year == self.date_dim_df.year), how="inner")
        new_df2.cache()
        new_df1 = new_df2.withColumnRenamed("id", "date_id")
        usa_data_df = new_df1.select("date_id", "state_id", "submit_date", "new_deaths", "new_cases", "total_deaths", "total_cases")
        if usa_data_df.count() == 0:
            return
        self.DButils.save_to_db(usa_data_df, self.config.covid_usa_fact)
        record_count = usa_data_df.count()
        self.DButils.insert_update_metadata(self.config.covid_usa_sector, record_count, max_date, self.config.covid_usa_sector, self.sector_type)
        logging.info("successfully loaded covid USA data till %s", max_date)
        # except Exception as ex:
        #     logging.error("Error transform data transform_load_usa_data %s", ex)
        #     raise ex


if __name__ == "__main__":
    transform_load = TransformLoad()
    transform_load.transform_load_data()
