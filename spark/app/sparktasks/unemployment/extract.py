import logging

from sparktasks.utils.config import Config
import os
from pyspark.sql import SparkSession
from sparktasks.utils.DBUtils import DButils
import requests
import pandas as pd
from os.path import isfile, join
import os, uuid
from datetime import date
from pyspark.sql.types import *
import json
from pyspark.sql.functions import year, month, dayofmonth, udf
import  pyspark.sql.functions as F


from sparktasks.utils.utils import UdfUtils


class Extract:
    logger = logging.getLogger('sparktasks.employment.Extract')
    api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    start_year = "2007"
    end_year = date.today().year
    state_api = "https://download.bls.gov/pub/time.series/la/la.area"

    UnemploymentData = {
        "unemployment_by_race": ["LNU00000003", "LNU01000003", "LNU01300003", "LNU03000003", "LNU04000003", "LNU00000006",
                                 "LNU01000006", "LNU01300006", "LNU03000006", "LNU04000006", "LNU00032183", "LNU01032183",
                                 "LNU01332183", "LNU03032183", "LNU04032183", "LNU00000009", "LNU01000009", "LNU01300009",
                                 "LNU03000009", "LNU04000009"],
        "Unemployment_By_Education_Level": ["LNU01027659", "LNU01327659", "LNU03027659", "LNU04027659", "LNU01027660",
                                            "LNU01327660", "LNU03027660", "LNU04027660", "LNU01027689", "LNU01327689",
                                            "LNU03027689", "LNU04027689", "LNU01027662", "LNU01327662", "LNU03027662",
                                            "LNU04027662"],
        "Unemployment_By_Industry": ["LNU03000000", "LNU04000000", "LNU03032231", "LNU04032231", "LNU03032232", "LNU04032232",
                                     "LNU03032235", "LNU04032235", "LNU03032236", "LNU04032236", "LNU03032239", "LNU04032239",
                                     "LNU03032240", "LNU04032240", "LNU03032241", "LNU04032241", "LNU03035109", "LNU04035109"]
    }

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('ExtractCovid') \
            .getOrCreate()
            #.config("spark.ui.port", "4070") \

        self.config = Config()
        self.metadata_df = self.DButils.load_from_db(self.spark, self.config.metadata)
        self.metadata_df.createGlobalTempView("metadata")

    # op_kwargs , use provide_context=True
    # dict_persons = {person['name']: person for person in list_persons}

    def get_filter_and_start_year(self, key):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == key.lower()).first()
        if row:
            record_date = row[4]
            if record_date.month == 12:
                return record_date.year+1
            return record_date.year
        return self.start_year

    def filter_df(self, df_to_filter, key):
        row = self.metadata_df.filter(self.metadata_df.sector_sub_type == key.lower()).first()
        if row:
            record_date = row[4]
            if record_date.month == 12:
                return df_to_filter.filter((df_to_filter.Results_series_data_period >='M01')
                                           & (df_to_filter.Results_series_data_year>record_date.year))
            if record_date.month<10:
                month_name = 'M0'+str(record_date.month)
            else:
                month_name = 'M'+str(record_date.month)
            return df_to_filter.filter((df_to_filter.Results_series_data_period>month_name)
                                       & (df_to_filter.Results_series_data_year>=record_date.year))
        return df_to_filter

    def extract_from_source(self):
        self.extract_unemployment_data()
        self.extract_state_rate_data()

    def extract_unemployment_data(self):
        data_dir = self.config.data_dir
        for key in self.UnemploymentData:
            try:
                series_ids = self.UnemploymentData[key]
                headers = {'Content-type': 'application/json'}
                start_year = self.get_filter_and_start_year(key.lower())
                data = json.dumps({"seriesid": series_ids, "startyear": start_year,
                                   "endyear": self.end_year})
                response = requests.post(self.api_url, data=data, headers=headers)
                file_name = "{0}.json".format(key)
                file_path = join(data_dir, file_name)
                with open(file_path, 'wb') as outf:
                    outf.write(response.content)
            except Exception as ex:
                logging.error("Error extracting data extract_state_rate %s", ex)
                raise ex

    def extract_state_rate_data(self):
        try:
            logging.info("about to extract employment data by state")
            df = pd.read_table(self.state_api)
            area_list = []
            for i in range(df.shape[0]):
                state = df.iloc[i]["area_code"]
                if state.startswith("ST"):
                    area_list.append("LAS" + state + "03")
            for i in range(0, len(area_list), 25):
                file_name_key = "Unemployment_By_State_" + str(i)
                file_name = "{0}.json".format(file_name_key)
                file_path = join(self.config.data_dir, file_name)
                logging.info("About to download unemployment data")
                headers = {'Content-type': 'application/json'}
                start_year= self.get_filter_and_start_year("unemployment_by_state_0")
                data = json.dumps({"seriesid": area_list[i:i + 25], "startyear": start_year,
                                   "endyear": self.end_year})
                response = requests.post(self.api_url, data=data, headers=headers)
                with open(file_path, 'wb') as outf:
                    outf.write(response.content)
                logging.info("Extracted employment state data to %s",file_path)
        except Exception as ex:
            logging.error("Error extracting data extract_state_rate %s", ex)
            raise ex

    def store_raw_in_db(self):
        try:
            logging.info("Writing employment data to raw table")
            data_dir = self.config.data_dir
            for f in os.listdir(data_dir):
                file_path = join(data_dir, f)
                if f.endswith("json"):
                    name = f.split(".")[0]
                    logging.info("Writing employment data to raw table %s", file_path)
                    start_year = self.get_filter_and_start_year(name.lower())
                    spark_df = self.spark.read.json("file:////" + file_path, multiLine=True)
                    flatten_df = self.flatten(spark_df)
                    split_udf = udf(lambda d: UdfUtils.get_code(d), StringType())
                    flatten_df = flatten_df.withColumn('Results_series_seriesID', split_udf(flatten_df.Results_series_seriesID))
                    flatten_df = self.filter_df(flatten_df,name.lower())
                    if flatten_df.count==0:
                        continue
                    final_df = flatten_df.select("message","responseTime","status","Results_series_seriesID",
                                                 "Results_series_data_period","Results_series_data_periodName",
                                                 "Results_series_data_value","Results_series_data_year")
                    self.DButils.save_to_db(final_df, self.config.get_config('RAW', name))

        except Exception as ex:
            logging.error("Error extracting data extract_state_rate %s", ex)
            raise ex

    def flatten(self, df):
        try:
            complex_fields = dict([(field.name, field.dataType)
                                   for field in df.schema.fields
                                   if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
            while len(complex_fields) != 0:
                col_name = list(complex_fields.keys())[0]
                print("Processing :" + col_name + " Type : " + str(type(complex_fields[col_name])))

                # if StructType then convert all sub element to columns.
                # i.e. flatten structs
                if (type(complex_fields[col_name]) == StructType):
                    expanded = [F.col(col_name + '.' + k).alias(col_name + '_' + k) for k in [n.name for n in complex_fields[col_name]]]
                    df = df.select("*", *expanded).drop(col_name)
                elif (type(complex_fields[col_name]) == ArrayType):
                    df = df.withColumn(col_name, F.explode_outer(col_name))

                # recompute remaining Complex Fields in Schema
                complex_fields = dict([(field.name, field.dataType)
                                       for field in df.schema.fields
                                       if type(field.dataType) == ArrayType or type(field.dataType) == StructType])
            return df
        except Exception as ex:
            logging.error("Error extracting data flatten %s", ex)
            raise ex

if __name__ == "__main__":
    extract = Extract()
    extract.extract_from_source()
    extract.store_raw_in_db()