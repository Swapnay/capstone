import pandas as pd
import requests
dls = "https://www.bls.gov/cpi/tables/supplemental-files/news-release-table1-202009.xlsx"
resp = requests.get(dls)

with open('../docs/bls_cpi_download.xlsx', 'wb') as writer:
    writer.write(resp.content)

pd.set_option('display.max_columns', 7)
cpi_data = pd.read_excel("../docs/bls_cpi_download.xlsx")

cpi_data.to_csv("../docs/bls_cpi.csv",encoding='utf-8', index=False)
cpi_data_csv = pd.read_csv("../docs/bls_cpi.csv")
#[["Date_reported", "Country", "New_cases","Cumulative_cases","New_deaths",  "Cumulative_deaths"]]
print(cpi_data_csv.columns)
#print(covid_world_data[["Date_reported", " Country", " New_cases"," Cumulative_cases"," New_deaths",  " Cumulative_deaths"]][100:110])

#covid_world_data.to_csv("../docs/who_covid_world.csv")
print(cpi_data_csv[10:20])