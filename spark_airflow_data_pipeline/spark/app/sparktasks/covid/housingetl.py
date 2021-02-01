from pyspark.sql import SparkSession
from pyspark import SparkFiles
from spark.app.sparktasks.utils.config import Config
import os
from spark.app.sparktasks.utils.DBUtils import DButils
import configparser
from pathlib import Path
import logging
from datetime import datetime
from pyspark.sql.functions import array, explode, struct, udf, when
from pyspark.sql.types import ArrayType, StructField, StructType, StringType, IntegerType, FloatType, DateType
from pyspark.sql.functions import year, month, dayofmonth
import pyspark.sql.functions as F


def split_str(metro_str):
    return metro_str.split(',')[0]


class Housing:

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder.appName('Extract_Covid').getOrCreate()
        global pd_date_df
        # pd_date_df = self.date_dim_df.toPandas()
        self.housing_date_dim = self.DButils.load_from_db(self.spark, self.config.get_config('DIMS', 'HOUSING_DATE_DIM'))
        self.state_df = self.DButils.load_from_db(self.spark, self.config.get_config('DIMS', 'STATE_DIM'))
        # self.state_df.createGlobalTempView("state_dim")
        # self.country_df = self.DButils.load_from_db(self.spark, Covid19etl.config.get('DIMS', 'COUNTRY_DIM'))
        # self.country_df.createGlobalTempView("country_dim")
        logging.info("initialization done")
        # Replace nagative values

    '''correctNegativeDiff = f.udf(lambda diff: 0 if diff < 0 else diff, LongType())'''

    def extract_raw(self):
        # covid_data.to_csv(path + "/resources/data/" + key + ".csv")
        self.write_raw(self.config.housing_price)
        #split_udf = udf(lambda d: split_str(d), StringType())
        #self.write_raw(self.config.housing_inventory, split_udf)

    def write_raw(self, type_config, split_udf=None):
        path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
        print(type_config)
        housing_dict = dict(type_config)
        for name, value in housing_dict.items():
            housing_path = path + "/resources/data/" + name + ".csv"
            # url = Covid19etl.config['EXTRACT'][name]
            file_name = os.path.basename(housing_path)
            self.spark.sparkContext.addFile(housing_path)
            housing_us_df = self.spark.read.csv('file://' + housing_path, header=True, inferSchema=True)
            housing_us_df.filter(housing_us_df.StateName.isNull())
            if split_udf is not None:
                housing_us_df = housing_us_df.withColumn("RegionName", split_udf(housing_us_df.RegionName))
            print(housing_us_df.columns)
            housing_us_df.fillna(0)
            self.DButils.save_to_db(housing_us_df, self.config.get_config('RAW', name))

    def create_dim(self, sql_query, dim_table_name):
        city_dim_df = self.spark.sql(sql_query)
        self.DButils.save_to_db(city_dim_df, dim_table_name)

    def create_city_dim(self, name):
        view_name = "{}.{}".format("global_temp", name)
        sql_city_dim = "SELECT  RegionName AS city_name,StateName AS state_name,CountyName as county_name FROM {}" \
                       " GROUP BY city_name,state_name,county_name".format(view_name)
        self.create_dim(sql_city_dim, self.config.get_config('DIMS', 'CITY_DIM'))

    def create_metro_dim(self, name):
        view_name = "{}.{}".format("global_temp", name)
        sql_metro_dim = "SELECT  Metro AS metro_city_name,StateName AS state_name,CountyName as county_name FROM {} " \
                        "GROUP BY metro_city_name,state_name,county_name".format(view_name)
        self.create_dim(sql_metro_dim, self.config.get_config('DIMS', 'METRO_DIM'))

    def create_county_dim(self,name):
        view_name = "{}.{}".format("global_temp", name)
        sql_county_dim = "SELECT  CountyName AS county_name,StateName AS state_name FROM {} " \
                         "GROUP BY county_name,state_name".format(view_name)
        self.create_dim(sql_county_dim, self.config.get_config('DIMS', 'COUNTY_DIM'))

    def transform_load_data_mysql(self):
        housing_dict = dict(self.config.housing_price)
        self.transform_load_data(housing_dict,'price', True)
        #housing_dict = dict(self.config.housing_inventory)
        #self.transform_load_data(housing_dict,'value', False)

    def transform_load_data(self,housing_dict,value_column,is_city=False):
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "False")
        for name in housing_dict.keys():
            raw = self.config.get_config('RAW', name)
            housing_df = self.DButils.load_from_db(self.spark, raw)
            housing_df.cache()
            housing_df.createGlobalTempView(name)
            view_name = "{}.{}".format("global_temp", name)
            self.spark.read.table(view_name)
            if self.config.get_config('DIMS','HOUSING_DIMS_LOADED') == '0':
                self.create_city_dim(name)
                self.create_metro_dim(name)
                self.create_county_dim(name)
                self.config.set_config('DIMS','HOUSING_DIMS_LOADED','1')

            city_dim_df = self.DButils.load_from_db(self.spark, self.config.get_config('DIMS', 'CITY_DIM'))
            metro_df = self.DButils.load_from_db(self.spark, self.config.get_config('DIMS', 'METRO_DIM'))
            county_df = self.DButils.load_from_db(self.spark, self.config.get_config('DIMS', 'COUNTY_DIM'))
            sql_housing_fact = "SELECT * FROM {}".format(view_name)
            housing_df = self.spark.sql(sql_housing_fact)
            housing_df.cache()
            if is_city:
                housing_df = housing_df.join(city_dim_df, (housing_df.RegionName == city_dim_df.city_name) &
                                               (housing_df.StateName == city_dim_df.state_name)
                                               & (housing_df.CountyName == city_dim_df.county_name), how="inner").withColumnRenamed("id", "city_id")
            housing_city_metro = housing_df.join(metro_df, (housing_df.Metro == metro_df.metro_city_name) & (housing_df.StateName == metro_df.state_name)
                                                   & (housing_df.CountyName == metro_df.county_name), how="inner").withColumnRenamed("id", "metro_id")
            housing_metro_state = housing_city_metro.join(self.state_df, (housing_city_metro.StateName == self.state_df.code)).withColumnRenamed("id", "state_id")
            if is_city:
                housing_metro_state = housing_metro_state.join(county_df, (housing_city_metro.CountyName == county_df.county_name)
                                                                     & (housing_df.StateName == county_df.state_name)).withColumnRenamed("id", "county_id")
            record_count = 0
            for i in range(9, len(housing_metro_state.columns)):
                inventory_date = str(housing_metro_state.columns[i])
                try:
                    date_time_obj = datetime.strptime(inventory_date, '%Y-%m-%d')
                except Exception as ex:
                    print(ex)
                    continue;
                # date_args = (date_time_obj.month, date_time_obj.year, date_time_obj.month, date_time_obj.year)
                housing_final = housing_metro_state.withColumnRenamed(inventory_date, value_column)
                housing_final =housing_final.withColumn('inventory_date', F.lit(date_time_obj))\
                    .withColumn('inventory_type', F.lit(name.lower()))
                housing_df = housing_final.withColumn("year", year(housing_final.inventory_date)) \
                    .withColumn("month", month(housing_final.inventory_date))
                housing_df =housing_df.join(self.housing_date_dim, (housing_df.year == self.housing_date_dim.year) & \
                                                                                   (housing_df.month == self.housing_date_dim.month)).withColumnRenamed('id', 'date_id')
                if is_city:
                    housing_final_df = housing_df.select('city_id', 'metro_id', 'date_id', 'inventory_date', 'state_id', 'county_id', 'inventory_type', 'price')
                else:
                    housing_final_df = housing_df.select('metro_id', 'date_id', 'inventory_date', 'state_id', 'inventory_type', 'value')
                record_count = record_count + housing_final_df.count()
                self.DButils.save_to_db(housing_final_df, self.config.get_config('FACTS', 'HOUSING_PRICE'))
                self.DButils.insert_update_metadata(name,record_count,date_time_obj,"")

            # for i in range(9,len(housing_metro_state.columns)):


Housing = Housing()
#Housing.extract_raw()
Housing.transform_load_data_mysql()
