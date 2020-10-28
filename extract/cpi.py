import pandas as pd
import requests
dls = "https://www.bls.gov/cpi/tables/supplemental-files/news-release-table1-202009.xlsx"
resp = requests.get(dls)

with open('../datasets/bls_cpi_download.xlsx', 'wb') as writer:
    writer.write(resp.content)

pd.set_option('display.max_columns', 7)
cpi_data = pd.read_excel("../datasets/bls_cpi_download.xlsx")
cpi_data =cpi_data.drop([0, 1])
print(cpi_data)

cpi_data.to_csv("../datasets/bls_cpi.csv",encoding='utf-8', index=False)
cpi_data_csv = pd.read_csv("../datasets/bls_cpi.csv")
print(cpi_data_csv.columns)

print(cpi_data_csv[4:20])