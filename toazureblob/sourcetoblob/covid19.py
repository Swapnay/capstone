import pandas as pd
from pyspark.sql.types import *
from toazureblob.sabutils import sablobutils
import logging
from toazureblob.sourcetoblob.parent_source import ParentSource



class Covid19(ParentSource):
    covid_config = {
        "covid_us_data": "https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD",
        "covid_world_data": "https://covid.ourworldindata.org/data/owid-covid-data.csv",

    }
    logger = logging.getLogger('pyspark')

    def create_schema(self, usa=True):
        if usa:
            return StructType([
                # Define the name field
                StructField('submission_date', TimestampType(), False),
                # Add the age field
                StructField('state', StringType(), True),
                # Add the city field
                StructField('tot_cases', IntegerType(), True),
                StructField('conf_cases', FloatType(), True),
                # Add the age field
                StructField('prob_cases', FloatType(), True),
                # Add the city field
                StructField('new_case', IntegerType(), True),
                StructField('pnew_case', FloatType(), True),
                # Add the age field
                StructField('tot_death', IntegerType(), True),
                # Add the city field
                StructField('conf_death', FloatType(), True),
                StructField('prob_death', FloatType(), True),
                # Add the age field
                StructField('new_death', IntegerType(), True),
                # Add the city field
                StructField('pnew_death', FloatType(), True),
                StructField('created_at', DateType(), True),
                # Add the city field
                StructField('consent_cases', StringType(), True),
                StructField('consent_deaths', StringType(), True)
            ])

    def parse_date_time(selfself, covid_data, us_data):
        if us_data == "covid_us_data":
            covid_data[["submission_date", "created_at"]] = covid_data[["submission_date", "created_at"]].apply(pd.to_datetime)
        else:
            covid_data[["date"]] = covid_data[["date"]].apply(pd.to_datetime)
        return covid_data

    def extract(self):

            for key in Covid19.covid_config:
                try:
                    self.logger.debug("starting Covid19 data %s",key)
                    covid_data = pd.read_csv(Covid19.covid_config[key], index_col=False)
                    covid_data.fillna(0)
                    print(covid_data.dtypes)
                    covid_data = self.parse_date_time(covid_data, key)
                    sablobutils.write_to_blob(covid_data, key)
                except Exception as ex:
                    self.logger.error(ex)

    def upload_to_azure_blob(self):
        self.extract()
