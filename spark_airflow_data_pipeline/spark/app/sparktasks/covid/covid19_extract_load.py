
from pyspark.sql import SparkSession
from pyspark import SparkFiles
import os
from spark.app.sparktasks.utils.DBUtils import DButils
import configparser
from pathlib import Path
import logging
from datetime import datetime
from pyspark.sql.functions import array, explode, struct, udf, when
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import year, month, dayofmonth


def convert_to_date(dateTimeStr):
    return datetime.strptime(dateTimeStr, '%m/%d/%Y')


def get_state_id(df, code):
    try:
        state_id = df[(df.code == code)].iloc[0]["id"]
        return state_id
    except Exception as ex:
        print(code)
        print(ex)
        return 60

    return df[(df.code == code)].iloc[0]["id"]


class Covid19etl:
    logger = logging.getLogger('sparktasks.covid.Covid19etl')
    config = configparser.RawConfigParser()
    print(os.path.dirname(os.path.abspath(__file__)))
    print(os.path.abspath(__file__))
    config.read_file(open("/usr/local/spark/app/sparktasks/config.cfg"))
    mysql_user = config.get('MYSQL', 'USER')
    mysql_password = config.get('MYSQL', 'PASSWORD')


    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('Extract_Covid').getOrCreate()
        self.date_dim_df = self.DButils.load_from_db(self.spark, Covid19etl.config.get('DIMS', 'DATE_DIM'))
        self.date_dim_df.createGlobalTempView("date_dim")
        global pd_date_df
        pd_date_df = self.date_dim_df.toPandas()
        self.state_df = self.DButils.load_from_db(self.spark, Covid19etl.config.get('DIMS', 'STATE_DIM'))
        self.state_df.createGlobalTempView("state_dim")
        self.country_df = self.DButils.load_from_db(self.spark, Covid19etl.config.get('DIMS', 'COUNTRY_DIM'))
        self.country_df.createGlobalTempView("country_dim")
        logging.info("initialization done")
        #Replace nagative values
    '''correctNegativeDiff = f.udf(lambda diff: 0 if diff < 0 else diff, LongType())

    df = df.withColumn('time_diff_1', correctNegativeDiff(df.time_diff_1))'''
    def extract(self):
        for name in Covid19etl.config['EXTRACT']:
            url = Covid19etl.config['EXTRACT'][name]
            file_name = os.path.basename(url)
            self.spark.sparkContext.addFile(url)
            confirmed_us_df = self.spark.read.csv('file://' + url, header=True, inferSchema=True)
            confirmed_us_df.fillna(0)
            self.DButils.save_to_db(confirmed_us_df, Covid19etl.config.get('RAW', name))

        '''Row(iso_code='AFG', continent='Asia', location='Afghanistan', date='2020-02-24', total_cases=1.0, new_cases=1.0, new_cases_smoothed=None,
         total_deaths=None, new_deaths=None, new_deaths_smoothed=None, total_cases_per_million=0.026, new_cases_per_million=0.026, new_cases_smoothed_per_million=None,
          total_deaths_per_million=None, new_deaths_per_million=None, new_deaths_smoothed_per_million=None, reproduction_rate=None, icu_patients=None, 
          icu_patients_per_million=None, hosp_patients=None, hosp_patients_per_million=None, weekly_icu_admissions=None, weekly_icu_admissions_per_million=None,
           weekly_hosp_admissions=None, weekly_hosp_admissions_per_million=None, total_tests=None, new_tests=None, total_tests_per_thousand=None, 
           new_tests_per_thousand=None, new_tests_smoothed=None, new_tests_smoothed_per_thousand=None, positive_rate=None, tests_per_case=None, tests_units=None, 
           total_vaccinations=None, new_vaccinations=None, new_vaccinations_smoothed=None, total_vaccinations_per_hundred=None, new_vaccinations_smoothed_per_million=None,
            stringency_index=8.33, population=38928341.0, population_density=54.422, median_age=18.6, aged_65_older=2.581, aged_70_older=1.337, gdp_per_capita=1803.987, extreme_poverty=None,
            cardiovasc_death_rate=597.029, diabetes_prevalence=9.59, female_smokers=None, male_smokers=None, handwashing_facilities=37.746, hospital_beds_per_thousand=0.5, life_expectancy=64.83,
             human_development_index=0.498)
            CREATE TABLE covid_usa_fact
            (
                id SERIAL PRIMARY KEY,
            date_id BIGINT UNSIGNED,
            state_id BIGINT UNSIGNED,
            submit_date TIMESTAMP NOT NULL,
            new_deaths INT,
            new_cases INT,
            total_cases INT,
            total_deaths INT,
            FOREIGN KEY (date_id) REFERENCES covid_date_dim(id),
                                             FOREIGN KEY (state_id) REFERENCES state_dim(id),
                                                                               UNIQUE (date_id,state_id)
            )ENGINE=InnoDB;
            tot_cases,new_case=0, tot_death=0, new_death=0'''

    def transform_data(self):
        for name in Covid19etl.config['EXTRACT']:
            pass

    def transform_load_usa_data(self):
        # spark = SparkSession.builder.appName('Transform').getOrCreate()
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "False")
        usa_df1 = self.DButils.load_from_db(self.spark, Covid19etl.config.get('RAW', "COVID_US_DATA"))
        # usa_df.select("submission_date")
        usa_df1.cache()
        print(usa_df1.count())

        usa_df1.printSchema()
        self.date_dim_df.printSchema()
        # usa_df.join(self.date_dim_df,)
        df_state = self.state_df.toPandas()
        idf = df_state[(df_state.code == 'RMI')].iloc[0]["id"]
        date_udf = udf(lambda d: convert_to_date(d), DateType())
        state_udf = udf(lambda d: get_state_id(df_state, d), IntegerType())
        df = globals()['pd_date_df']


        '''.withColumn("state",state_udf(usa_df.state)) 
        .withColumn("date_id",date_id_udf(usa_df.submission_date))"date_id",'''

        usa_df = usa_df1.join(self.state_df, usa_df1.state == self.state_df.code)
        usa_df.cache()
        print(usa_df.count())
        usa_df.show(100)
        new_df = usa_df.withColumn("submit_date", date_udf(usa_df.submission_date))\
            .withColumnRenamed("id", "state_id") \
            .withColumnRenamed("new_death", "new_deaths") \
            .withColumnRenamed("tot_death", "total_deaths") \
            .withColumnRenamed("new_case", "new_cases") \
            .withColumnRenamed("tot_cases", "total_cases") \


        # df = new_df.withColumn("state_id",state_udf(new_df.state_id)) \
        usa_df2 = new_df.withColumn("year", year(new_df.submit_date)) \
            .withColumn("month", month(new_df.submit_date)) \
            .withColumn("day", dayofmonth(new_df.submit_date))
        print(usa_df2.printSchema())
        usa_df2.cache()
        print(usa_df2.count())
        #usa_df2.show(20)
        self.date_dim_df.show(100)
        new_df2 = self.date_dim_df.join(usa_df2, (usa_df2.day == self.date_dim_df.day) & (usa_df2.month == self.date_dim_df.month) & (usa_df2.year == self.date_dim_df.year),how="inner")
        new_df2.show(100)
        new_df2.cache()
        print(new_df2.count())
        new_df1 = new_df2.withColumnRenamed("id", "date_id")
        usa_data_df = new_df1.select("date_id", "state_id", "submit_date", "new_deaths", "new_cases", "total_deaths", "total_cases")

        self.DButils.save_to_db(usa_data_df, Covid19etl.config.get('FACTS', "COVID_US_DATA"))

    def get_state_id(self, code):
        query = 'select id from state_dim WHERE code={} '.format(code)
        state_id = self.spark.sql(query)
        return state_id.id

        # date_udf = udf(lambda d: convert_date(d), DateType())
        # df = df.withColumn('date', date_udf(df['date']))
        # df = df.withColumnRenamed('Province/State', 'Province_State') \
        #     .withColumnRenamed('Country/Region', 'Country_Region') \
        #     .withColumnRenamed('Long', 'Long_')


covid19etl = Covid19etl()
covid19etl.extract()
covid19etl.transform_load_usa_data()
