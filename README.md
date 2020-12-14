
# Effect of Covid-19 on  Economy (work-in-progress)
## 1. Problem Definition
The COVID-19 virus pandemic has led to unexpected interruptions in economic activity around the world.In efforts to slow down the spread of COVID-19, many states and countries shut down several economic areas such as tourism and entertainment venues, restaurants, personal services, and some manufacturing facilities. This has led many areas into a recession. According to UNIDO, the economic crisis unleashed by the outbreak of COVID-19 is hurting economies, regardless of income level.
In this capstone will visualize the progression of COVID-19 in terms of number of cases and deaths along with its effect on areas such as stock market, employment, CPI, food and agriculture. Time series data will be collected from various  sources that represent these area.
This capstone will try to provide visual view for below questions:

**Stock market:**

- The most raised / dropped stocks
- The most stable/unstable stocks

**CPI:**

- How consumer spending has changed
- Areas of increased/decreased spending 

**Employment:**

- Industries that have majority job losses.
- Education level and income range of majority un employed.

**Housing Market:**

- How housing prices changed in each city
- How Inventory changed and sales are moving (Metro & US)
 
**food and agriculture:**

## 2. Expected Results
Since the data is continuous, Current plan is to use line chart, column chart and combination of both.

## 3 Data sets
In this section, we describe in detail how to get data from datasources.

**Covid-19**

- Two data sets involved: global and the US.

- Data source:[USA](https://data.cdc.gov) and [World](https://covid19.who.int/)

- Type: Time series

- Supported methods:  csv

- Frequency: daily

Interested Data:

- For each country/State we interested in:

  - Population
  - Total cases
  - Total deaths
  - Total new cases

Sample code:
```
import pandas as pd
pd.set_option('display.max_columns', 7)
covid_data = pd.read_csv("https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD")
print(covid_data[["submission_date", "state", "tot_cases", "new_case", "tot_death",]][100:110])
covid_data.to_csv("../docs/cdc_covid.csv")

US Raw data sample:
    submission_date state  tot_cases  new_case  tot_death
 100      05/01/2020    CO      15668       486        820
 101      05/02/2020    CO      16120       452        832
 102      05/03/2020    CO      16534       414        842
 103      05/04/2020    CO      16878       344        851
 104      05/05/2020    CO      17317       439        903
 105      05/06/2020    CO      17738       421        921
 106      05/07/2020    CO      18318       580        944
 107      05/08/2020    CO      18793       475        960
 108      05/09/2020    CO      19316       523        967
 109      05/10/2020    CO      19632       316        971
 
 covid_world_data = pd.read_csv("https://covid19.who.int/WHO-COVID-19-global-data.csv")
 print(covid_world_data.columns)
 print(covid_world_data[["Date_reported", " Country", " New_cases"," Cumulative_cases"," New_deaths",  " Cumulative_deaths"]][100:110])
 covid_world_data.to_csv("../docs/who_covid_world.csv")
 
 
World data:
  Date_reported      Country   New_cases   Cumulative_cases   New_deaths      Cumulative_deaths
 100    2020-04-12  Afghanistan          34                555            3   18
 101    2020-04-13  Afghanistan         110                665            3   21 
 102    2020-04-14  Afghanistan          49                714            2   23
 103    2020-04-15  Afghanistan          70                784            2   25  
 104    2020-04-16  Afghanistan          10                794            4   29 
 105    2020-04-17  Afghanistan          51                845            1   30
 106    2020-04-18  Afghanistan          63                908            0   30
 107    2020-04-19  Afghanistan          88                996            3   33
 108    2020-04-20  Afghanistan           0                996            0   33
 109    2020-04-21  Afghanistan          96               1092            3   36                   
 ```


**Stock Prices:**

Type: Time series

Supported methods: API, json

Frequency: daily

iexfinance API can be used to extract the data.This API takes stock ticker as input.  


```
 from iexfinance.refdata import get_symbols
 from iexfinance.stocks import Stock
 from datetime import datetime
 from iexfinance.stocks import get_historical_data
 
 start = datetime(2016, 1, 1)
 end = datetime(2019, 7, 30)
 print(get_historical_data("AAPL", start, end,
                            output_format='pandas',
                                token='<TOKEN>'))
Stock price data sample
             open   high    low  close     volume
date                                             
2016-01-04  25.65  26.34  25.50  26.34  270597548
2016-01-05  26.44  26.46  25.60  25.68  223163968
2016-01-06  25.14  25.59  24.97  25.18  273829552
2016-01-07  24.67  25.03  24.11  24.11  324377712
2016-01-08  24.64  24.78  24.19  24.24  283192064
 ```
**Consumer Price Index:**

- Data Source: [source](https://www.bls.gov/cpi/tables/supplemental-files/news-release-table1-202009.xlsx)
- Interested in knowing how consumer spending has changed during covid.
Sample:

```import pandas as pd
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
```


**Employment / Unemployment rate:**

- DataSource: [source](https://www.bls.gov/webapps/legacy/cpsatab14.htm)
- Type: Time series - monthly
- Interested Series:
  - Unemployment by industry 
  - Unemployment by occupation
  - employment by education level
  - Employment by race
  - Total unemployment
  
 Sample:
 ```
import requests
import json
import prettytable
headers = {'Content-type': 'application/json'}
data = json.dumps({"seriesid": ['LNU03032229'],"startyear":"2011", "endyear":"2014"})
p = requests.post('https://api.bls.gov/publicAPI/v2/timeseries/data/', data=data, headers=headers)
json_data = json.loads(p.text)
for series in json_data['Results']['series']:
    x=prettytable.PrettyTable(["series id","year","period","value","footnotes"])
    seriesId = series['seriesID']
    for item in series['data']:
        year = item['year']
        period = item['period']
        value = item['value']
        footnotes=""
        for footnote in item['footnotes']:
            if footnote:
                footnotes = footnotes + footnote['text'] + ','
        if 'M01' <= period <= 'M12':
            x.add_row([seriesId,year,period,value,footnotes[0:-1]])
output = open('../datasets/'+seriesId + '.txt','w')
output.write (x.get_string())
output.close()
```
Output:

```
+-------------+------+--------+-------+-----------+
|  series id  | year | period | value | footnotes |
+-------------+------+--------+-------+-----------+
| LNU03032229 | 2014 |  M12   |  6341 |           |
| LNU03032229 | 2014 |  M11   |  6413 |           |
| LNU03032229 | 2014 |  M10   |  6580 |           |
| LNU03032229 | 2014 |  M09   |  6766 |           |
| LNU03032229 | 2014 |  M08   |  7200 |           |
| LNU03032229 | 2014 |  M07   |  7366 |           |
| LNU03032229 | 2014 |  M06   |  7164 |           |
| LNU03032229 | 2014 |  M05   |  7243 |           |
| LNU03032229 | 2014 |  M04   |  7105 |           |
| LNU03032229 | 2014 |  M03   |  8110 |           |
| LNU03032229 | 2014 |  M02   |  8479 |           |
| LNU03032229 | 2014 |  M01   |  8437 |           |
| LNU03032229 | 2013 |  M12   |  7662 |           |
| LNU03032229 | 2013 |  M11   |  7882 |           |
| LNU03032229 | 2013 |  M10   |  8011 |           |
| LNU03032229 | 2013 |  M09   |  8216 |           |
| LNU03032229 | 2013 |  M08   |  8391 |           |
| LNU03032229 | 2013 |  M07   |  8683 |           |
| LNU03032229 | 2013 |  M06   |  8800 |           |
| LNU03032229 | 2013 |  M05   |  8655 |           |
| LNU03032229 | 2013 |  M04   |  8608 |           |
| LNU03032229 | 2013 |  M03   |  9148 |           |
| LNU03032229 | 2013 |  M02   |  9826 |           |
| LNU03032229 | 2013 |  M01   | 10435 |           |
| LNU03032229 | 2012 |  M12   |  9104 |           |
| LNU03032229 | 2012 |  M11   |  8600 |           |
| LNU03032229 | 2012 |  M10   |  8892 |           |
| LNU03032229 | 2012 |  M09   |  8935 |           |
| LNU03032229 | 2012 |  M08   |  9552 |           |
| LNU03032229 | 2012 |  M07   |  9692 |           |
| LNU03032229 | 2012 |  M06   |  9626 |           |
| LNU03032229 | 2012 |  M05   |  9419 |           |
| LNU03032229 | 2012 |  M04   |  9197 |           |
| LNU03032229 | 2012 |  M03   | 10106 |           |
| LNU03032229 | 2012 |  M02   | 10517 |           |
| LNU03032229 | 2012 |  M01   | 10736 |           |
| LNU03032229 | 2011 |  M12   |  9956 |           |
| LNU03032229 | 2011 |  M11   |  9740 |           |
| LNU03032229 | 2011 |  M10   | 10126 |           |
| LNU03032229 | 2011 |  M09   | 10375 |           |
| LNU03032229 | 2011 |  M08   | 10524 |           |
| LNU03032229 | 2011 |  M07   | 10515 |           |
| LNU03032229 | 2011 |  M06   | 10733 |           |
| LNU03032229 | 2011 |  M05   | 10628 |           |
| LNU03032229 | 2011 |  M04   | 10560 |           |
| LNU03032229 | 2011 |  M03   | 11288 |           |
| LNU03032229 | 2011 |  M02   | 11641 |           |
| LNU03032229 | 2011 |  M01   | 11778 |           |
+-------------+------+--------+-------+-----------+

```
**Housing Market:**
  - DataSource: [Zillow](https://www.zillow.com/research/data/)
  - Type: Time series - monthly/weekly
  - Interested Series:
    - Top tier for a region 65th to 95th percentile range for a given region
    - Bottom-tier  (typical value for homes that fall within the 5th to 35th percentile range for a given region).
    - Typical value for all single-family homes in a given region
    - For-Sale Inventory
    - Days to Pending
    - Median Sale Price
    
Sample
```
import pandas as pd
pd.set_option('display.max_columns', 7)
housing_data = pd.read_csv("http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv")
print(housing_data.columns)
print(housing_data[["RegionName", "StateName", "CountyName", '2020-05-31', '2020-06-30', '2020-07-31', '2020-08-31', '2020-09-30' ]][10:20])
housing_data.to_csv("../datasets/housing_usa.csv")


 RegionName StateName            CountyName  ...  2020-07-31  \         2020-08-31
10         Austin        TX         Travis County  ...    668559.0      675088.0
11       San Jose        CA    Santa Clara County  ...   1408057.0      1434708.0
12   Jacksonville        FL          Duval County  ...    298747.0      300912.0
13   Indianapolis        IN         Marion County  ...    254294.0      257201.0
14  San Francisco        CA  San Francisco County  ...   2071877.0      2076277.0
15      Charlotte        NC    Mecklenburg County  ...    456816.0      461385.0
16     Fort Worth        TX        Tarrant County  ...    317164.0      319543.0
17         Tucson        AZ           Pima County  ...    347079.0      350456.0
18       Columbus        OH       Franklin County  ...    271790.0      274608.0
19     Louisville        KY      Jefferson County  ...    307231.0      309979.0

```

## 4. Data model
Using star schema.Data model is [here](https://github.com/Swapnay/capstone/tree/master/data_model/erd)

## 5. Data Exploration
[Here](https://github.com/Swapnay/capstone/blob/master/eda/Covid_Economy_impact.ipynb)

## 6 Spark data pipeline
1.[Here](https://github.com/Swapnay/capstone/tree/master/toazureblob)
## References
[Reference1](https://www.uaex.edu/life-skills-wellness/health/covid19/COVID-Economic_Impacts_in_Arkansas.aspx)
