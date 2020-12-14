import requests
import pandas as pd
from os.path import isfile, join
import os, uuid
import json
from toazureblob.sabutils import sablobutils
from pyspark.sql.types import *
from datetime import date
import logging
from toazureblob.sourcetoblob.parent_source import ParentSource


class UnemploymentData(ParentSource):
    api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    start_year = "2011"
    end_year = date.today().year
    state_api = "https://download.bls.gov/pub/time.series/la/la.area"
    logger = logging.getLogger('pyspark')


    series = {
        "Unemployment_By_Race": ["LNU00000003", "LNU01000003", "LNU01300003", "LNU03000003", "LNU04000003", "LNU00000006",
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

    def each_series(self):
        foot_notes_schema = StructType([
            StructField('code', StringType(), True),
            StructField('text', StringType(), True)
        ])
        inside_json_schema = StructType([
            StructField('year', IntegerType(), True),
            StructField('period', StringType(), True),
            StructField('periodName', StringType(), True),
            # Add the age field
            StructField('latest', BooleanType(), True),
            # Add the city field
            StructField('value', LongType(), True),
            StructField('footnotes', ArrayType(foot_notes_schema), True)
        ])
        return StructType([
            # Define the name field
            StructField('seriesID', StringType(), False),
            # Add the age field
            StructField('data', ArrayType(inside_json_schema), False)
            # Add the city fiel
        ])

    def all_series(self):
        return StructType([
            StructField('series', ArrayType(self.each_series()), True)])

    def create_schema(self):

        result = StructType([
            # Define the name field
            StructField('Results', self.all_series(), False),
            # Add the age field
            StructField('message', ArrayType(StringType()), True),
            StructField('responseTime', LongType(), True),
            StructField('status', StringType(), True)])
        return result

    def extract_by_type(self):

            data_dir = self.get_data_dir()
            for key in UnemploymentData.series:
                try:
                    series_ids = UnemploymentData.series[key]
                    self.logger.debug("Get data for %s", key)
                    headers = {'Content-type': 'application/json'}
                    data = json.dumps({"seriesid": series_ids, "startyear": self.start_year, "endyear": self.end_year})
                    response = requests.post(self.api_url, data=data, headers=headers)
                    file_name = "{0}.json".format(key)
                    file_path = join(data_dir, file_name)
                    with open(file_path, 'wb') as outf:
                        outf.write(response.content)
                except Exception as ex:
                    self.logger.error(ex)

    def get_data_dir(self):
        path1 = os.path.abspath("")
        data_fol = f'{path1}/toazureblob/data/'
        print(data_fol)
        return data_fol

    def extract_state_rate(self):
        try:
            data_dir = self.get_data_dir()
            df = pd.read_table(self.state_api)
            sablobutils.write_to_blob(df, "Unemployment_State_Ids")
        except Exception as ex:
            self.logger.error(ex)
        area_list = []
        for i in range(df.shape[0]):
            state = df.iloc[i]["area_code"]
            if state.startswith("ST"):
                # LAS is code for localarea unemployment and 03 is unemployment rate
                area_list.append("LAS" + state + "03")
        for i in range(0, len(area_list), 25):
            try:
                file_name_key = "Unemployment_By_State_" + str(i)
                file_name = "{0}.json".format(file_name_key)
                file_path = join(data_dir, file_name)
                self.logger.debug("About to download unemployment data %s", str(i))
                headers = {'Content-type': 'application/json'}
                data = json.dumps({"seriesid": area_list[i:i + 25], "startyear": "2010", "endyear": self.end_year})
                response = requests.post(self.api_url, data=data, headers=headers)

                with open(file_path, 'wb') as outf:
                    outf.write(response.content)
            except Exception as ex:
                self.logger.error(ex)

    def write_to_blob(self):
        try:
            data_dir = self.get_data_dir()
            for f in os.listdir(data_dir):
                file_path = join(data_dir, f)
                if f.endswith("json"):
                    name = f.split(".")[0]
                    spark = sablobutils.get_spark_session()
                    spark_df = spark.read.json("file:////" + file_path, multiLine=True)
                    flatten_df = sablobutils.flatten(spark_df)
                    self.logger.debug("Writing data to azure %s", name)
                    sablobutils.write_spark_df_to_blob(flatten_df, name)
        except Exception as ex:
            self.logger.error(ex)


    def upload_to_azure_blob(self):
        sablobutils.read_spark_df_to_blob()
        self.extract_state_rate()
        self.extract_by_type()
        self.write_to_blob()
