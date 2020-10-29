import pandas as pd
pd.set_option('display.max_columns', 7)
housing_data = pd.read_csv("http://files.zillowstatic.com/research/public_v2/zhvi/City_zhvi_uc_sfrcondo_tier_0.67_1.0_sm_sa_mon.csv")
print(housing_data.columns)
print(housing_data[["RegionName", "StateName", "CountyName", '2020-05-31', '2020-06-30', '2020-07-31', '2020-08-31', '2020-09-30' ]][10:20])
housing_data.to_csv("../datasets/housing_usa.csv")
