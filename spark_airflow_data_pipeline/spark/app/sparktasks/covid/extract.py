
import pandas as pd
import os


class CovidData:
    covid_config = {
        #"covid_us_data": "https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD",
        "covid_world_data": "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    }
    def extract(self):
        for key in CovidData.covid_config:
            covid_data = pd.read_csv(CovidData.covid_config[key])
            path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            #covid_data.to_csv(path + "/resources/data/" + key + ".csv")
            path =path +"/resources/data/" + key + ".csv"
            covid_data.to_csv(path)

covid = CovidData()
covid.extract()