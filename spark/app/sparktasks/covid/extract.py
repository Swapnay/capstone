import logging

from spark.app.sparktasks.utils.config import Config
from pyspark.sql import SparkSession
from spark.app.sparktasks.utils.DBUtils import DButils
import pandas as pd
import os


class Extract:
    logger = logging.getLogger('sparktasks.covid.Extract')

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('Extract').config("spark.ui.port", "4080").getOrCreate()
        self.config = Config()
        self.metadata_df = self.DButils.load_from_db(self.spark, self.config.metadata)
        self.metadata_df.createGlobalTempView("metadata")

    # op_kwargs , use provide_context=True
    # dict_persons = {person['name']: person for person in list_persons}

    #def extract_covid19(self):
        # ti = context['ti']
        # metadata_dictionary = ti.xcom_pull(task_ids='extract_data',key="dag_last_run_details")
        #self.extract_from_source()

    def extract_from_source(self):  # column_name,
        try:
            housing_dict = dict(self.config.covid19_source)
            for key, value in housing_dict.items():
                logging.info("extract raw data  from  %s", value)
                covid_data = pd.read_csv(value)
                # row = self.metadata_df.filter(self.metadata_df.sector_sub_type == key.lower()).first()
                # if row:
                #     record_date = row[1]
                #     covid_data = covid_data[(covid_data['submission_date'] > record_date)]
                path = os.path.join(self.config.data_dir, key + ".csv")
                covid_data.to_csv(path, index=False)
        except Exception as ex:
            logging.error("Error extracting data %s", ex)
            raise ex

    def store_raw_in_db(self):
        try:
            for name, value in dict(self.config.covid19_source).items():
                logging.info("store raw data  %s", name)
                covid_usa = os.path.join(self.config.data_dir , name + ".csv")
                url = covid_usa
                file_name = os.path.basename(url)
                if os.path.isfile(url):
                    self.spark.sparkContext.addFile(url)
                    confirmed_us_df = self.spark.read.csv('file://' + url, header=True, inferSchema=True)
                    confirmed_us_df = confirmed_us_df.fillna(0)
                    self.DButils.save_to_db(confirmed_us_df, self.config.get_raw_by_sector(name),mode='overwrite')
        except Exception as ex:
            logging.error("Error store covid ata in raw  %s", ex)
            raise ex


if __name__ == "__main__":
    extract = Extract()
    #extract.extract_from_source()
    extract.store_raw_in_db()
