import requests
import json
import pandas as pd
from dbconfig import engine


class UnemploymentData:
    api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    start_year = "2011"
    end_year = "2020"

    series = {
        "by_race": ["LNU00000003", "LNU01000003", "LNU01300003", "LNU03000003", "LNU04000003", "LNU00000006",
                    "LNU01000006", "LNU01300006", "LNU03000006", "LNU04000006", "LNU00032183", "LNU01032183",
                    "LNU01332183", "LNU03032183", "LNU04032183", "LNU00000009", "LNU01000009", "LNU01300009",
                    "LNU03000009", "LNU04000009"],
        "by_education_level": ["LNU01027659", "LNU01327659", "LNU03027659", "LNU04027659", "LNU01027660",
                               "LNU01327660", "LNU03027660", "LNU04027660", "LNU01027689", "LNU01327689",
                               "LNU03027689", "LNU04027689", "LNU01027662", "LNU01327662", "LNU03027662",
                               "LNU04027662"],
        "by_industry": ["LNU03000000", "LNU04000000", "LNU03032231", "LNU04032231", "LNU03032232", "LNU04032232",
                        "LNU03032235", "LNU04032235", "LNU03032236", "LNU04032236", "LNU03032239", "LNU04032239",
                        "LNU03032240", "LNU04032240", "LNU03032241", "LNU04032241", "LNU03035109", "LNU04035109"]
    }

    def extract(self):
        for key in UnemploymentData.series:
            series_ids = UnemploymentData.series[key]
            headers = {'Content-type': 'application/json'}
            data = json.dumps({"seriesid": series_ids, "startyear": self.start_year, "endyear": self.end_year})
            response = requests.post(self.api_url, data=data, headers=headers)
            with open("../datasets/" + key + ".json", 'wb') as outf:
                outf.write(response.content)

    def extract_state_rate(self):
        df =pd.read_table(  "https://download.bls.gov/pub/time.series/la/la.area")
        df.to_sql('unemployment_series__dim', con = engine, if_exists = 'append', index= id)
        area_list =[]
        for i in range(df.shape[0]):
            state = df.iloc[i]["area_code"]
            if state.startswith("ST"):
            #LAS is code for localarea unemployment and 03 is unemployment rate
                area_list.append("LAS" + state +"03")
        print(area_list)
        for i in range(0,len(area_list),25):
            headers = {'Content-type': 'application/json'}
            data = json.dumps({"seriesid": area_list[i:i+25], "startyear": "2010", "endyear": 2020})
            response = requests.post(self.api_url, data=data, headers=headers)
            with open("../datasets/unemployment_state" +str(i) +".json", 'wb') as outf:
                outf.write(response.content)


if __name__ == "__main__":
    unemplyment = UnemploymentData()
    unemplyment.extract()
