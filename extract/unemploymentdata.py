import requests
import json


class UnemploymentData:
    api_url = "https://api.bls.gov/publicAPI/v2/timeseries/data/"
    start_year = "2000"
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


if __name__ == "__main__":
    unemplyment = UnemploymentData()
    unemplyment.extract()
