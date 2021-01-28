import configparser
from pathlib import Path
import os
import logging

class DButils:
    logger = logging.getLogger('sparktasks.db.DButils')
    config = configparser.RawConfigParser()
    config.read_file(open("/usr/local/spark/app/sparktasks/config.cfg"))
    mysql_user = config.get('MYSQL', 'USER')
    mysql_password = config.get('MYSQL', 'PASSWORD')
    mysql_host = config.get('MYSQL', 'HOST')
    mysql_port = config.get('MYSQL', 'PORT')
    mysql_db_name = config.get('MYSQL', 'DATABASE')
    driver_name = config.get('MYSQL', 'DRIVER')
    connection_str_jdbc = 'jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true'.format(mysql_host, mysql_port, mysql_db_name)

    def load_from_db(self, spark, table_name):
        spark_df = spark.read.format('jdbc').options(
            url=DButils.connection_str_jdbc,
            driver=DButils.driver_name,
            dbtable=table_name,
            user=DButils.mysql_user,
            password=DButils.mysql_password).load()
        return spark_df

    def save_to_db(self, spark_df, table_name):
        try:
            spark_df.write.format('jdbc').options(
                url=DButils.connection_str_jdbc,
                driver=DButils.driver_name,
                dbtable=table_name,
                user=DButils.mysql_user,
                password=DButils.mysql_password).mode('append').save()
        except ValueError:
            self.logger.error(f'Error querying table {table_name} ')


#     confirmed_us_df.write.format('jdbc').options(
#             url=connection_str_jdbc,
#             driver="com.mysql.cj.jdbc.Driver",
#             dbtable="covid_usa_data_raw",
#             user='root',
#             password='Mi4man11').save()
#
# dataframe_mysql = spark.read.format("jdbc").options(
#     url=connection_str_jdbc,
#     driver = "com.mysql.jdbc.Driver",
#     dbtable = "covid_usa_data",
#     user="root",
#     password="Mi4man11").load()


