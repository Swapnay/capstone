import configparser
from pathlib import Path
import os
import logging
from spark.app.sparktasks.utils.config import Config
import mysql.connector
import pandas as pd
from numpy import record
from datetime import date

class DButils:
    logger = logging.getLogger('sparktasks.utils.DButils')

    def __init__(self):
        self.config = Config()

    def load_from_db(self, spark, table_name):
        spark_df = spark.read.format('jdbc').options(
            url=self.config.connection_str_jdbc,
            driver=self.config.driver_name,
            dbtable=table_name,
            user=self.config.mysql_user,
            password=self.config.mysql_password).load()
        return spark_df

    def save_to_db(self, spark_df, table_name):
        try:
            spark_df.write.format('jdbc').options(
                url=self.config.connection_str_jdbc,
                driver=self.config.driver_name,
                dbtable=table_name,
                user=self.config.mysql_user,
                password=self.config.mysql_password).mode('append').save()
        except ValueError:
            self.logger.error(f'Error querying table {table_name} ')

    def get_connection(self):
        connection = None
        try:
            connection = mysql.connector.connect(user=self.config.mysql_user,
                                                 password=self.config.mysql_password,
                                                 host=self.config.mysql_host,
                                                 port=self.config.mysql_port,
                                                 database=self.config.mysql_db_name)
        except Exception as error:
            print("Error while connecting to database",error)

        return connection

    def insert_update_metadata(self, type_val,records,record_date,other_data=""):
        sl =("INSERT INTO metadata(type,execution_date,number_records_added,record_date,other_data) " 
            "VALUES(%s,%s,%s,%s,%s ) ON DUPLICATE KEY UPDATE type=%s,execution_date=%s,number_records_added=%s,record_date=%s,other_data=%s")
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            insert_val = (type_val,date.today(),records,record_date,other_data)
            cursor.execute(sl, insert_val)
        except Exception as e:
            print(e)
        connection.commit()
        cursor.close()







