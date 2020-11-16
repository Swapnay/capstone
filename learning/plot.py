import matplotlib.pyplot as plt
import pandas as pd
from dbconfig import engine

#GE,CCL,COTY,OXY,HFC,AAL fell
#AMZN,AAPL,FDX,NVDA,UPS,WST,DXCM,EBAY

covid_data = pd.read_sql('select max(submission_date) as date,extract(MONTH from submission_date) '
                             'as month,avg(new_cases) as new_cases,max(total_cases) as total_cases from covid_world_fact where country_id = 190 '
                             'group by month order by date', engine,parse_dates=True, index_col="date")
change = pd.read_sql("select city_id, inventory_date,inventory_type,price,difference from"
                      "(SELECT city_id,inventory_date,price,inventory_type,extract(year from inventory_date) as year1,"
                      "price-Lag(price, 1) OVER(PARTITION BY city_id,inventory_type ORDER BY "
                      "extract(month from inventory_date),extract(year from inventory_date)) as difference "
                      "FROM home_prices_normalized_fact where extract(year from inventory_date) in (2019,2020) "
                      "and city_id in (43,6,5,4,42)) as rate_change where rate_change.year1=2020",engine,parse_dates=True, index_col="inventory_date")
#change = pd.read_sql('select stock_id,stock_date,closing_price from stock_prices_fact where stock_id in (30,87,128,209,229,350) '
                #     ,engine,parse_dates=True, index_col="stock_date")
#'GE','CCL','COTY','OXY','HFC','AAL' Energy,inductrial
state_list=[43,6,5,4,42]
#city =change.loc[change["inventory_type"]=='mid_tier']
#type_list =['mid_tier','top_tier','bottom_tier','single_family','condo','1bd','2bd','3bd','4bd','5bd']
color_list=["red","green","purple","pink","aqua","brown"]
#AMZN,AAPL,FDX,NVDA,UPS,WST,DXCM,EBAY
#state_list=[347,141,162,187,347,463,488]
#color_list=["red","purple","yellow","aqua","pink","brown","olive"]

fig, ax = plt.subplots()

# Add the time-series for "relative_temp" to the plot

ax.plot(covid_data.index, covid_data['new_cases'],color="blue")
ax.set_xlabel('Time')
ax.set_ylabel('Covid-19 new  Cases')

ax2 = ax.twinx()
ax2.set_ylabel('home price change')
for i,j in zip(state_list,color_list):
    df=change.loc[change["city_id"]==i]
    df1=df.loc[df['inventory_type']=='5bd']
    # Plot the relative temperature in red
    ax2.plot(df1.index, df1["difference"], color=j)


plt.show()

