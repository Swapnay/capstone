
# COVID - 19 and Affected Sectors Data Warehouse
## 1. Problem Definition
The COVID-19 virus pandemic has led to unexpected interruptions in economic activity around the world.In efforts to slow down the spread of COVID-19, many states and countries shut down several economic areas such as tourism and entertainment venues, restaurants, personal services, and some manufacturing facilities. This has led many areas into a recession. According to UNIDO, the economic crisis unleashed by the outbreak of COVID-19 is hurting economies, regardless of income level.

In this capstone we will Extract, Transform and Load the data. Data will then be used to visualize the progression of COVID-19 in terms of number of cases and deaths along with its effect on areas such as the stock market, unemployment rate, housing market. Time series data will be collected from various sources that represent these areas. This capstone will try to provide correlation between below questions.


|Sector|  COVID - 19   | S&P 500 Stock         |  Housing Market| Unemployment Rate|
| ------------- | ------------- |---------|--------|----------------|  
|| World/ USA cases | The most raised / dropped stocks  |How housing prices/Inventory changed in each city/state| Unemployment rate by Industry/ Race/ state|


## 2. Expected Results
Time Series Data warehouse.Data that can be used for analysis using line chart, column chart and combination of both.

## 2. Architecture
![Architecture](https://github.com/Swapnay/capstone/blob/master/docs/architecture.png)

## 3 Datasets
In this section, we describe in detail how to get data from data source.
All data selected is time series data

|Sector|  COVID - 19   | S&P 500 Stock prices        |  Housing Market| Unemployment Rate|
| ------------- | ------------- |---------|--------|----------------|  
|Source| [World Data](https://covid19.who.int/) [USA Data](https://data.cdc.gov)| Yahoo Finance  |[Zillow.com](https://www.zillow.com/research/data/)|[bls.gov](https://www.bls.gov/webapps/legacy/cpsatab14.htm)|
| Frequency| Daily |Daily|Monthly|Monthly|
|Data Type|CSV|CSV|CSV|Table/JSON|

**Detailed information about data sources is captured in [Data-Extraction](https://github.com/Swapnay/capstone/wiki/Data-Extraction) **


## 4. Data Extraction (Landing Zone):
- Extracted Data files are saved to the file system.
- Raw tables with that data are created in Azure mysql
Example landing zone in cloud (Amazon S3 / Azure Blob storage)
More information on different zones[Data-Zones](https://github.com/Swapnay/capstone/wiki/Data-Model) 
## 5 Data Transformation 
Reasons for using Apache Spark for data transformation:
- Spark is capable of handling several petabytes of data at a time, distributed across a cluster
of thousands of cooperating physical or virtual servers.
- Supports languages such as Java, Python, R, and Scala and integration of leading storage solutions.
- Simple to use with rich set of APIs
- Speed
[More about the configuration is here](https://github.com/Swapnay/capstone/wiki/Data-Transformation-Using-Apache-Spark)

## 6. Processed Zone (Data Load):
- Data from the landing zone is transformed using a processing engine and loaded into structured tables.
- In this case it is mysql /Azure mysql.
More information on different zones [Data-Zones](https://github.com/Swapnay/capstone/wiki/Data-Model) 

## 7. Data model 
[Star Schema data model is Here](https://github.com/Swapnay/capstone/blob/master/data_model/erd/capstone.pdf)
Reasons for using Star Schemas
 - Queries are simpler
 - Easier business insights reporting
 - Better-performing queries: 
 - Provides data to OLAP systems: 
 
## 8. Initial Data Exploration
Initial Data exploratory analysis suggests An interesting correlation between COVID-19 and affected sectors.
 [EDA is Here](https://github.com/Swapnay/capstone/blob/master/eda/Covid_Economy_impact.ipynb)
 
## 9 ETL/ELT workflow Orchestration 
Now that we have ETL pipeline in place, how do we  schedule daily or monthly while integrating all dependencies
Apache Airflow is used for below reasons: 
- Dynamic Pipeline Generation ( DAG configuration as code )
- Extensibility and Functionality
- Easily integrates with all cloud platforms 
- Large user base

More details on [project wiki](https://github.com/Swapnay/capstone/wiki/ETL--ELT-Workflow-Orchestration) 
and [DAGS folder](https://github.com/Swapnay/capstone/tree/master/dags)

ETL DAG run for Stocks data
![Example-Dag-Run](https://github.com/Swapnay/capstone/blob/master/docs/dag-exec/Stocks.png)

## 10 Spark and Azure Mysql Optimisation
In the development lifecycle, More time is spent on optimising and maintaining the code than on writing it.
Following were main areas of optimisation
- Code optimization
- Increase Compute size and storage Azure Mysql
- Dynamically selecting batch size based on source.
- Spark optimization

[More on optimisation is Here](https://github.com/Swapnay/capstone/wiki/Optimization---Spark-and-Azure-Mysql)

## 11 REST API 

 * Java Springboot REST API.
 * API interfaces to Stocks,COVID, Unemployment Rate, and Housing data monthly average tables.
 * Spring Boot, Swagger, JPA
 * [For More Info Checkout Here](https://github.com/Swapnay/capstone/blob/master/rest-API/Readme.md)

## 12 Jupyter Notebook Integration - Correlation plots
* Docker jupyter notebook integration using image jupyter/pyspark-notebook enables end user analyse data.
* Users can either use spark in driver mode or submit  to spark clusters running in standalone mode.
* End users may also call above REST API to get aggregated data.
Observations from data loaded:

For more plots [Look at python note book](https://github.com/Swapnay/capstone/blob/master/notebooks/capstone_final.ipynb)
[And also exploratory data analysis](https://github.com/Swapnay/capstone/blob/master/eda/Covid_Economy_impact.ipynb)

* The Leisure and Hospitality sector had most job losses. Construction, Transportation and utilities is second in the list.
![Sector](https://github.com/Swapnay/capstone/blob/master/docs/images/sector.png)

* The States with the most unemployment rate - Nevada, Hawaii have the most unemployment rate as these 2 states depend on tourism.
  In addition Nevada , like Michigan, is also dependent on Manufacturing and Mining.
![State](https://github.com/Swapnay/capstone/blob/master/docs/images/Unemployment-covid.png)

## 12 Resources
* [Slide Deck](https://drive.google.com/file/d/1wvZu4Kef1g3AuXII4j7UoqYcxFocQB_O/view?usp=sharing)
* [Project Instructions](https://github.com/Swapnay/capstone/tree/master/docker)
* [Project WIKI](https://github.com/Swapnay/capstone/wiki/COVID---19---Economy-impact)
* [Star Schema data model is Here](https://github.com/Swapnay/capstone/blob/master/data_model/erd/capstone.pdf)]
* [Data analysis](https://github.com/Swapnay/capstone/blob/master/notebooks/)
* [REST API](https://github.com/Swapnay/capstone/blob/master/rest-API/Readme.md)




## References
[Reference1](https://www.uaex.edu/life-skills-wellness/health/covid19/COVID-Economic_Impacts_in_Arkansas.aspx)
[Reference2](https://www.xplenty.com/blog/snowflake-schemas-vs-star-schemas-what-are-they-and-how-are-they-different/#:~:text=Benefits%20of%20Star%20Schemas&text=Better%2Dperforming%20queries%3A%20By%20removing,schemas%20to%20build%20OLAP%20cubes.)
[Reference3](https://developer.hpe.com/blog/spark-101-what-is-it-what-it-does-and-why-it-matters/)
[Reference4](https://www.syntelli.com/eight-performance-optimization-techniques-using-spark)
