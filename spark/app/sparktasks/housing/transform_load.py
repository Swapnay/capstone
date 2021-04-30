import logging
from spark.app.sparktasks.utils.config import Config
from pyspark.sql import SparkSession
from pyspark.sql.functions import year, month, dayofmonth
from spark.app.sparktasks.utils.DBUtils import DButils
import pyspark.sql.functions as F
from datetime import datetime


class TransformLoad:
    logger = logging.getLogger('sparktasks.housing.TransformLoad')
    sector_type = 'HOUSING'

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder.appName('HousingTransformLoad') \
            .getOrCreate()
            #.config("spark.ui.port", "4080")\

        self.spark.conf.set("spark.sql.crossJoin.enabled", "True")
        self.housing_date_dim = self.DButils.load_from_db(self.spark, self.config.housing_date_dim)
        self.state_df = self.DButils.load_from_db(self.spark, self.config.state_dim)
        self.city_dim_df = self.DButils.load_from_db(self.spark, self.config.city_dim)
        self.metro_df = self.DButils.load_from_db(self.spark, self.config.metro_dim)
        self.county_df = self.DButils.load_from_db(self.spark, self.config.county_dim)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        logging.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT * FROM {} WHERE sector_type = '{}' ORDER BY execution_date desc limit 20) foo""".format(self.config.metadata,
                                                                                                         self.sector_type)

    def create_dim(self, sql_query, dim_table_name):
        city_dim_df = self.spark.sql(sql_query)
        self.DButils.save_to_db(city_dim_df, dim_table_name)

    def create_city_dim(self, name):
        view_name = "{}.{}".format("global_temp", name)
        sql_city_dim = "SELECT  RegionName AS city_name,StateName AS state_name,CountyName AS county_name " \
                       "FROM {}" \
                       " GROUP BY city_name,state_name,county_name".format(view_name)
        self.create_dim(sql_city_dim, self.config.city_dim)

    def create_metro_dim(self, name):
        view_name = "{}.{}".format("global_temp", name)
        sql_metro_dim = "SELECT  Metro AS metro_city_name,StateName AS state_name,CountyName AS county_name " \
                        "FROM {} " \
                        "GROUP BY metro_city_name,state_name,county_name".format(view_name)
        self.create_dim(sql_metro_dim, self.config.metro_dim)

    def create_county_dim(self, name):
        view_name = "{}.{}".format("global_temp", name)
        sql_county_dim = "SELECT  CountyName AS county_name,StateName AS state_name FROM {} " \
                         "GROUP BY county_name,state_name".format(view_name)
        self.create_dim(sql_county_dim, self.config.county_dim)

    def transform_load_data(self):
        housing_dict = dict(self.config.housing_inventory)
        self.transform_load(housing_dict, 'value', False)
        housing_dict = dict(self.config.housing_price)
        self.transform_load(housing_dict, 'price', True)

    def get_table_query(self, name):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == name).first()
        if row:
            record_date = row[2]
            date_time = record_date.strftime("%Y-%m-%d")
            return "SELECT * FROM {} WHERE  inventory_date > '{}'".format(name, record_date)
        return name

    def transform_load(self, housing_dict, value_column, is_city=False):
        self.spark.conf.set("spark.sql.execution.arrow.enabled", "False")
        for name in housing_dict.keys():
            raw = self.config.get_config('RAW', name)
            logging.info("Extract load %s",raw)
            housing_df = self.DButils.load_from_db(self.spark, raw)
            housing_df.cache()
            housing_df.createGlobalTempView(raw)
            view_name = "{}.{}".format("global_temp", raw)
            self.spark.read.table(view_name)
            if self.county_df.count() == 0:
                self.create_city_dim(name)
                self.city_dim_df = self.DButils.load_from_db(self.spark, self.config.city_dim)
            if self.metro_df.count() == 0:
                self.create_metro_dim(name)
                self.metro_df = self.DButils.load_from_db(self.spark, self.config.metro_dim)
            if self.county_df.count() == 0:
                self.create_county_dim(name)
                self.county_df = self.DButils.load_from_db(self.spark, self.config.county_dim)
            sql_housing_fact = "SELECT * FROM {}".format(view_name)
            housing_df = self.spark.sql(sql_housing_fact)
            housing_df.cache()
            row = self.metadata_df.filter(self.metadata_df.sector_sub_type == name.lower()).first()
            columns = housing_df.columns
            if row:
                record_date = row[4]
                date_time = record_date.strftime("%Y-%m-%d")
                index_to = columns.index(date_time)
                index_from = columns.index('2018-01-31')
                if '1996-01-31' in columns:
                    index_from = columns.index('1996-01-31')
                elif '2004-09-30' in columns:
                    index_from = columns.index('2004-09-30')
                elif '2008-04-30' in columns:
                    index_from = columns.index('2008-04-30')
                elif '2018-01-31' in columns:
                    index_from = columns.index('2018-01-31')
                col_list = housing_df.columns[index_from:index_to + 1]
                housing_df = housing_df.drop(*col_list)


            if is_city:
                housing_df = housing_df.join(self.city_dim_df, (housing_df.RegionName == self.city_dim_df.city_name) &
                                             (housing_df.StateName == self.city_dim_df.state_name)
                                             & (housing_df.CountyName == self.city_dim_df.county_name),
                                             how="inner").withColumnRenamed("id", "city_id")
                housing_city_metro = housing_df.join(self.metro_df, (housing_df.Metro == self.metro_df.metro_city_name)
                                                     & (housing_df.StateName == self.metro_df.state_name)
                                                     & (housing_df.CountyName == self.metro_df.county_name), how="inner") \
                    .withColumnRenamed("id", "metro_id")
            else:
                housing_city_metro = housing_df.join(self.metro_df, (housing_df.RegionName == self.metro_df.metro_city_name)
                                                     & (housing_df.StateName == self.metro_df.state_name)
                                                     , how="inner").withColumnRenamed("id", "metro_id")

            housing_metro_state = housing_city_metro.join(self.state_df, (housing_city_metro.StateName == self.state_df.code)) \
                .withColumnRenamed("id", "state_id")
            if is_city:
                housing_metro_state = housing_metro_state.join(self.county_df,
                                                               (housing_city_metro.CountyName == self.county_df.county_name)
                                                               & (housing_df.StateName == self.county_df.state_name)) \
                    .withColumnRenamed("id", "county_id")
            record_count = 0

            for i in range(9, len(housing_metro_state.columns)):
                inventory_date = str(housing_metro_state.columns[i])
                try:
                    date_time_obj = datetime.strptime(inventory_date, '%Y-%m-%d')
                except Exception as ex:
                    logging.error("column is not a date column %s",ex)
                    continue
                # date_args = (date_time_obj.month, date_time_obj.year, date_time_obj.month, date_time_obj.year)
                housing_final = housing_metro_state.withColumnRenamed(inventory_date, value_column)
                housing_final = housing_final.withColumn('inventory_date', F.lit(date_time_obj)) \
                    .withColumn('inventory_type', F.lit(name.lower()))
                housing_df = housing_final.withColumn("year", year(housing_final.inventory_date)) \
                    .withColumn("month", month(housing_final.inventory_date))
                housing_df = housing_df.join(self.housing_date_dim, (housing_df.year == self.housing_date_dim.year) & \
                                             (housing_df.month == self.housing_date_dim.month)) \
                    .withColumnRenamed('id', 'date_id')
                if is_city:
                    housing_final_df = housing_df.select('city_id', 'metro_id', 'date_id', 'inventory_date', 'state_id',
                                                         'county_id', 'inventory_type', 'price')
                    self.DButils.save_to_db(housing_final_df, self.config.get_config('FACTS', 'HOUSING_PRICE'))
                else:
                    housing_final_df = housing_df.select('metro_id', 'date_id', 'inventory_date', 'state_id', 'inventory_type',
                                                         'value')
                    housing_final_df.show()
                    self.DButils.save_to_db(housing_final_df, self.config.get_config('FACTS', 'HOUSING_INVENTORY'))
                record_count = record_count + housing_final_df.count()
                self.DButils.insert_update_metadata(name.lower(), record_count, date_time_obj, name,self.sector_type)


if __name__ == "__main__":
    transform_load = TransformLoad()
    transform_load.transform_load_data()
