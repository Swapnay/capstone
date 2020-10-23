
# Effect of Covid-19 on  Economy (work-in-progress)
## 1. Problem Definition
The COVID-19 virus pandemic has led to unexpected interruptions in economic activity around the world.In efforts to slow down the spread of COVID-19, many states and countries shut down several economic areas such as tourism and entertainment venues, restaurants, personal services, and some manufacturing facilities. This has led many areas into a recession. According to UNIDO, the economic crisis unleashed by the outbreak of COVID-19 is hurting economies, regardless of income level.
In this capstone will visualize the progression of COVID-19 in terms of number of cases and deaths along with its effect on areas such as stock market, employment, CPI, food and agriculture. Time series data will be collected from various  sources that represent these area.
This capstone will try to provide visual view for below questions:

**Stock market:**

The most raised / dropped stocks
The most stable/unstable stocks

**CPI:**

How consumer spending has changed
Areas of increased/decreased spending 

**Employment:**

Industries that have majority job losses.
Education level and income range of majority un employed.

**food and agriculture:**

## 2. Expected Results
Since the data is continuous, Current plan is to use line chart, column chart and combination of both.

## 3 Datasets
In this section, we describe in detail how to get data from datasources.

**Covid-19**

Two datasets involved: global and the US.

Data source:https://data.cdc.gov and https://covid19.who.int/

Type: Time series

Supported methods:  csv

Frequency: daily

Interested features:

For each country/area/territory we interested in:

Population
Total cases
Total deaths
Tital new cases

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

**Employment / Unemployment rate:**

## 4. Data model
Using snowflake model
## References
https://www.uaex.edu/life-skills-wellness/health/covid19/COVID-Economic_Impacts_in_Arkansas.aspx
