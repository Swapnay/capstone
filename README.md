
# Effect of Covid-19 on  Economy (work-in-progress)
## 1. Problem Definition
The COVID-19 virus pandemic has led to unexpected interruptions in economic activity around the world.In efforts to slow down the spread of COVID-19, many states and countries shut down several economic areas such as tourism and entertainment venues, restaurants, personal services, and some manufacturing facilities. This has led many areas into a recession. According to UNIDO, the economic crisis unleashed by the outbreak of COVID-19 is hurting economies, regardless of income level.
In this capstone will visualize the progression of COVID-19 in terms of number of cases and deaths along with its effect on areas such as stock market, employment, CPI, food and agriculture. Time series data will be collected from various  sources that represent these area.
This capstone will try to provide visual view for below questions:

**Stock market:**

- The most raised / dropped stocks
- The most stable/unstable stocks

**Employment:**

- Industries that have majority job losses.
- Education level and income range of majority un employed.

**Housing Market:**

- How housing prices changed in each city
- How Inventory changed and sales are moving (Metro & US)
 
**food and agriculture:**

## 2. Expected Results
Since the data is continuous, Current plan is to use line chart, column chart and combination of both.

## 2. Architecture
![Architecture](https://drive.google.com/file/d/1ZZolWwqhpB_uUZ0Qi2G0prDDpBRM5U4a/view?usp=sharing)

## 3 Data sets
In this section, we describe in detail how to get data from datasources.
All data selected is time series data

|Sector|  COVID - 19   | S&P 500 Stock prices        |  Housing Market| Unemployment Rate|
| ------------- | ------------- |---------|--------|----------------|  
|Source| [World Data](https://covid19.who.int/)\n[USA DAte](https://data.cdc.gov)| Yahoo Finance  |[Zillow](https://www.zillow.com/research/data/|[Unemployment](https://www.bls.gov/webapps/legacy/cpsatab14.htm|
| Frequency| Daily |Daily|Monthly|Monthly|
|Data Type|CSV|CSV|CSV|Table/Json|
**Detailed information about data sources is captured in [Data-Extraction](https://github.com/Swapnay/capstone/wiki/Data-Extraction) **

## 4. Initial Data Exploration
Initial Data exploratory analysis suggests An interesting correlation between COVID-19 and affected sectors.
 EDA is [Here](https://github.com/Swapnay/capstone/blob/master/eda/Covid_Economy_impact.ipynb)
## 5. Landing Zone:
 - Extracted Data files are saved to file system.
 - Raw tables with that data are created in Azure mysql
 Example landing zone in cloud (Amazon S3 / Azure Blog storage)
More information on different zones [Data-Zones](https://github.com/Swapnay/capstone/wiki/Data-Model) 
## 6 Data Transformation using Apache Spark
1.[Here](https://github.com/Swapnay/capstone/tree/master/toazureblob)
## 6. Processed Zone (Data Load):
- Data from landing zone is transformed using processing engine and loaded in to structured tables.
- In this case it is mysql /Azure mysql.
More information on different zones [Data-Zones](https://github.com/Swapnay/capstone/wiki/Data-Model) 

## 7. Data model 
Star Schema data model is [Here](https://github.com/Swapnay/capstone/tree/master/data_model/erd)
Reasons for using Star Schemas
 - Queries are simpler
 - Easier business insights reporting
 - Better-performing queries: 
 - Provides data to OLAP systems: 
 
## 8 ETL/ELT workflow Orchestration 
Now that we have ETL pipeline in place, how do we  schedule daily or monthly while integrating all dependencies
Apache Airflow is used for below reasons: 
- Dynamic Pipeline Generation ( DAG configuration as code )
- Extensibility and Functionality
- Easily integrates with all cloud platforms 
- Large user base

More details on [project wiki](https://github.com/Swapnay/capstone/wiki/ETL-ELT-Workflow-Orchestration) 
and [DAGS folder](https://github.com/Swapnay/capstone/tree/master/dags)
ETL DAG run for Stocks data

![Example-Dag-Run](https://github.com/Swapnay/capstone/blob/master/docs/dag-exec/Stocks.png)
[Stocks Dag Run]

## 9 Rest API 

 * Java Springboot REST API.
 * API interfaces to Stocks,COVID, Unemployment Rate, and Housing data monthly average tables.
 * Spring Boot, Swagger, JPA
 [Instructions to checkout](https://github.com/Swapnay/capstone/blob/master/rest-API/Readme.md)

## 10 Jupyter Notebook Integration - Correlation plots 
* Docker jupyter notebook integration using image jupyter/pyspark-notebook enables end user analyse data.
* User can either use spark in driver mode or submit  to spark cluster running in standalone mode.
* End user may also call above REST API to get aggregated data.
Observations from data loaded:

* Leisure and Hospitality sector had most job losses. Construction, Transportation and utilities is second in the list.

![Sector](https://github.com/Swapnay/capstone/blob/master/docs/images/sector.png)

* States with most unemployment rate
![State](https://github.com/Swapnay/capstone/blob/master/docs/images/Unemployment-covid.png)



## References
[Reference1](https://www.uaex.edu/life-skills-wellness/health/covid19/COVID-Economic_Impacts_in_Arkansas.aspx)
[Reference2](https://www.xplenty.com/blog/snowflake-schemas-vs-star-schemas-what-are-they-and-how-are-they-different/#:~:text=Benefits%20of%20Star%20Schemas&text=Better%2Dperforming%20queries%3A%20By%20removing,schemas%20to%20build%20OLAP%20cubes.)
