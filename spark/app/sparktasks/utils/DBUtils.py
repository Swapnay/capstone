import logging
from sparktasks.utils.config import Config
import mysql.connector

from datetime import datetime
import time


class DButils:
    logger = logging.getLogger('sparktasks.utils.DButils')

    def __init__(self):
        self.config = Config()

    def load_from_db_with_partitions(self, spark, table_name, partition_column, lower_bound, upper,partitions=30):
        time_start = time.time()
        spark_df = spark.read.format('jdbc').options(
            url=self.config.connection_str_jdbc,
            driver=self.config.driver_name,
            dbtable=table_name,
            user=self.config.mysql_user,
            password=self.config.mysql_password,
            partitionColumn=partition_column,
            lowerBound=lower_bound,
            upperBound=upper,
            numPartitions=partitions).load()
        time_end = time.time()
        logging.info("It took this long to run load_from_db_with_partitions {}: Query {}".format((time_end - time_start), table_name))
        print("Time took to run load_from_db {} query {}".format(table_name, time_end - time_start))
        return spark_df

    def load_from_db(self, spark, table_name):
        time_start = time.time()
        spark_df = spark.read.format('jdbc').options(
            url=self.config.connection_str_jdbc,
            driver=self.config.driver_name,
            dbtable=table_name,
            user=self.config.mysql_user,
            password=self.config.mysql_password).load()
        time_end = time.time()
        print("Time took to run load_from_db {} query {}".format(table_name, time_end - time_start))
        return spark_df

    def save_to_db(self, spark_df, table_name, mode='append'):
        try:
            time_start = time.time()
            spark_df.write.format('jdbc').options(
                url=self.config.connection_str_jdbc,
                driver=self.config.driver_name,
                dbtable=table_name,
                user=self.config.mysql_user,
                password=self.config.mysql_password,
                batchsize=50000,
                isolationLevel='NONE',
                truncate=True
            ).mode(mode).save()
            time_end = time.time()
            print("Time took to DButils.save {} query {}".format(table_name, time_end - time_start))

        except ValueError as e:
            print(e)

    def save_to_db_with_partition(self, spark_df, table_name, mode='append',repartition_count=20,batch_size=50000, column='id'):
        try:
            time_start = time.time()
            spark_df.repartition(repartition_count,column).write.format('jdbc').options(
                url=self.config.connection_str_jdbc,
                driver=self.config.driver_name,
                dbtable=table_name,
                user=self.config.mysql_user,
                password=self.config.mysql_password,
                batchsize=batch_size,
                isolationLevel='NONE',
                truncate=True
            ).mode(mode).save()
            time_end = time.time()
            print("Time took to DButils.save {} query {}".format(table_name, time_end - time_start))

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

    def insert_update_metadata(self, sector_sub_type, number_records_added, record_date, other_data, sector_type):
        sl = ("INSERT INTO metadata(sector_sub_type,execution_date,number_records_added,record_date,other_data,sector_type) "
              "VALUES(%s,%s,%s,%s,%s,%s ) ON DUPLICATE KEY UPDATE number_records_added=VALUES(number_records_added),execution_date=values(execution_date),record_date=VALUES(record_date)")
        connection = self.get_connection()
        cursor = connection.cursor()
        try:
            time_start = time.time()
            insert_val = (sector_sub_type, datetime.now().date(), number_records_added, record_date, other_data, sector_type)
            cursor.execute(sl, insert_val)
        except Exception as e:
            print(e)
        finally:
            if connection:
                connection.commit()
                cursor.close()
                #connection.close()
        time_end = time.time()
        print("Time took to insert_update_metadata {} query {}".format(sl, time_end - time_start))

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
