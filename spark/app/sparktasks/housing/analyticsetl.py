from pyspark.sql import SparkSession
import logging
from sparktasks.utils.DBUtils import DButils
from sparktasks.utils.config import Config
from datetime import datetime, timedelta
import pyspark.sql.functions as F


class AnalyticsEtl:
    sector_type = 'HOUSING_ANALYTICS'
    logger = logging.getLogger('sparktasks.housing.AnalyticsEtl')

    def __init__(self):
        self.DButils = DButils()
        self.config = Config()
        self.spark = SparkSession.builder \
            .appName('Housing.AnalyticsEtl') \
            .getOrCreate()
            #.config("spark.ui.port", "4080") \

        self.spark.conf.set("spark.sql.shuffle.partitions", 60)
        self.metadata_df = self.DButils.load_from_db(self.spark, self.get_metadata_query())
        self.state_df = self.DButils.load_from_db(self.spark, self.config.state_dim)
        self.logger.info("initialization done")

    def get_metadata_query(self):
        return """(SELECT sector_type, sector_sub_type, max(record_date) as record_date \
                  FROM {} WHERE sector_type = '{}' \
                  GROUP BY sector_type, sector_sub_type ) foo""" \
            .format(self.config.metadata, self.sector_type)

    def get_table_query(self, name, sector_sub_type):
        record_date = self.get_record_start_date(sector_sub_type)
        date_time = record_date.strftime("%Y-%m-%d")
        return """(SELECT * FROM {} WHERE  inventory_date> '{}') foo""".format(name, date_time)

    def get_record_start_date(self, sector_sub_type):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == sector_sub_type).first()
        record_date = datetime.now()
        record_date = record_date.replace(year=2015, month=1, day=1)
        if row:
            record_date = row[2]
            record_date = record_date + timedelta(days=1)
        return record_date

    def transform_load_analytics_tables(self):
        self.transform_load_housing_inventory()
        self.transform_load_housing_price()

    def transform_load_housing_inventory(self):
        try:
            fact_table = self.config.housing_inventory_fact
            monthly_table = self.config.housing_inventory_monthly
            housing = self.DButils.load_from_db_with_partitions(self.spark, self.get_table_query(fact_table, monthly_table),
                                                                'inventory_date',self.get_record_start_date(monthly_table),datetime.now())
            housing.createOrReplaceTempView(monthly_table.lower())
            query = ("SELECT state_id," \
                     "inventory_type, year(inventory_date) as year," \
                     "month(inventory_date) as month, Date(inventory_date) AS inventory_date, " \
                     "percentile_approx(value,0.5) as days FROM {} " \
                     "GROUP BY state_id,inventory_type,year,month,inventory_date ORDER BY  state_id,inventory_type,year,month") \
                .format(monthly_table.lower())

            housing_df = self.spark.sql(query)
            if housing_df.count() == 0:
                return
            housing_df = housing_df.join(self.state_df, self.state_df.id == housing_df.state_id, how='inner') \
                .withColumnRenamed('code', 'state')
            housing_df = housing_df.withColumnRenamed('name', 'state_name')
            housing_df = housing_df.select('state', 'state_name', 'inventory_type', 'days', 'year', 'month', 'inventory_date')
            self.DButils.save_to_db(housing_df, monthly_table)
            max_date = housing_df.agg(F.max('inventory_date')).first()[0]
            self.DButils.insert_update_metadata(monthly_table, housing_df.count(), max_date,
                                                monthly_table, self.sector_type)
        except Exception as ex:
            self.logger.error("Error extracting data %s", ex)
            raise ex

    def transform_load_housing_price(self):
        try:
            fact_table = self.config.housing_price_fact
            monthly_table = self.config.housing_price_monthly
            housing = self.DButils.load_from_db(self.spark, self.get_table_query(fact_table, monthly_table))
                                                                #'inventory_date',self.get_record_start_date(monthly_table),
                                                                #datetime.now())
            housing.createOrReplaceTempView(monthly_table.lower())
            query = ("SELECT state_id," \
                     "inventory_type, year(inventory_date) as year," \
                     "month(inventory_date) as month, Date(inventory_date) AS inventory_date, " \
                     "percentile_approx(price,0.5) as avg_price FROM {} " \
                     "GROUP BY state_id,inventory_type,year,month,inventory_date ORDER BY  state_id,inventory_type,year,month") \
                .format(monthly_table.lower())

            housing_df = self.spark.sql(query)
            if housing_df.count() == 0:
                return
            housing_df = housing_df.join(self.state_df, self.state_df.id == housing_df.state_id, how='inner') \
                .withColumnRenamed('code', 'state')
            housing_df = housing_df.withColumnRenamed('name', 'state_name')
            housing_df = housing_df.select('state', 'state_name', 'inventory_type', 'avg_price', 'year', 'month', 'inventory_date')
            housing_df = housing_df.fillna(0)
            self.DButils.save_to_db(housing_df, monthly_table)
            max_date = housing_df.agg(F.max('inventory_date')).first()[0]
            self.DButils.insert_update_metadata(monthly_table, housing_df.count(), max_date,
                                                monthly_table, self.sector_type)
        except Exception as ex:
            self.logger.error("Error extracting data %s", ex)
            raise ex




if __name__ == "__main__":
    transform_load = AnalyticsEtl()
    transform_load.transform_load_analytics_tables()
