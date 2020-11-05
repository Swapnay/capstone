import pandas as pd

class CovidData:

    covid_config = {
    "covid_us_data":"https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD",
    "covid_world_data":"https://covid.ourworldindata.org/data/owid-covid-data.csv"
    }

    def extract(self):
        for key in CovidData.covid_config:
            covid_data = pd.read_csv(CovidData.covid_config[key])
            covid_data.to_csv("../datasets/"+ key +".csv")

if __name__ == "__main__":
    covid = CovidData()
    covid.extract()

