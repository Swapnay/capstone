import configparser
from pathlib import Path
import os
import logging
from sparktasks.utils.config import Config
import mysql.connector
import pandas as pd
from numpy import record
from datetime import datetime
from datetime import date


class DButils:
    logger = logging.getLogger('sparktasks.utils.DButils')

    def __init__(self):
        self.config = Config()

    def load_from_db(self, spark, table_name, partition, lower_bound, upper):
        spark_df = spark.read.format('jdbc').options(
            url=self.config.connection_str_jdbc,
            driver=self.config.driver_name,
            dbtable=table_name,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
            partitionColumn=partition,
            lowerBound=lower_bound,
            upperBound=upper).load()
        return spark_df

    def load_from_db(self, spark, table_name):
        spark_df = spark.read.format('jdbc').options(
            url=self.config.connection_str_jdbc,
            driver=self.config.driver_name,
            dbtable=table_name,
            user=self.config.mysql_user,
            password=self.config.mysql_password).load()
        return spark_df

    def save_to_db(self, spark_df, table_name,mode='append'):
        try:
            spark_df.write.format('jdbc').options(
                url=self.config.connection_str_jdbc,
                driver=self.config.driver_name,
                dbtable=table_name,
                user=self.config.mysql_user,
                password=self.config.mysql_password,
            ).mode(mode).save()
        except ValueError as e:
            print(e)

    def get_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(user=self.config.mysql_user,
                                                 password=self.config.mysql_password,
                                                 host=self.config.mysql_host,
                                                 port=self.config.mysql_port,
                                                 database=self.config.mysql_db_name)
        except Exception as error:
            print("Error while connecting to database", error)

        return connection

    def insert_update_metadata(self, sector_sub_type, number_records_added, record_date, other_data,sector_type):
        sl = ("INSERT INTO metadata(sector_sub_type,execution_date,number_records_added,record_date,other_data,sector_type) "
              "VALUES(%s,%s,%s,%s,%s,%s ) ON DUPLICATE KEY UPDATE number_records_added=VALUES(number_records_added),execution_date=values(execution_date),record_date=VALUES(record_date)")
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            insert_val = (sector_sub_type, datetime.now().date(), number_records_added, record_date, other_data, sector_type)
            cursor.execute(sl, insert_val)
        except Exception as e:
            print(e)
        finally:
            if connection:
                connection.commit()
                cursor.close()


    class MetadataUtils:

        def get_last_exec_date(self):  # , **context):
            # type_map = self.config.get_by_type(sector_type)

            # type_keys = "','".join(dict(type_map).keys())
            # types = '{}{}{}'.format("'",type_keys,"'")
            self.spark.read.table("global_temp.metadata")

            # sql = 'SELECT * FROM global_temp.metadata WHERE sector_sub_type IN ({}) ORDER BY execution_date DESC LIMIT {};'.format(types,length)
            sql = ' SELECT sector_sub_type, execution_date, record_date, other_data ' \
                  'FROM ' \
                  '(SELECT sector_sub_type, record_date,execution_date, other_data,rank() ' \
                  'OVER (PARTITION BY sector_sub_type ORDER BY execution_date DESC) as rank1 ' \
                  'FROM global_temp.metadata) tmp  WHERE rank1 = 1'
            spark_df = self.spark.sql(sql)
            df = spark_df.select('sector_sub_type', 'record_date', 'other_data')
            dict_val = map(lambda row: row.asDict(), df.collect())
            last_run_details = [(r['sector_sub_type'], r['record_date']) for r in dict_val]
            dim_loaded_details = [(r['sector_sub_type'], r['other_data']) for r in dict_val]
            # task_instance = context['ti']
            self.metadata_dicts = dict(last_run_details)
