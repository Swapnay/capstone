
from extract_transform_load.extract.covid_data import CovidData
from extract_transform_load.extract import housing_data
from extract_transform_load.load_mysql.covid import covid as covid_load
from extract_transform_load.load_mysql.housing import Housing
from extract_transform_load.load_mysql.stocks import Stocks
from extract_transform_load.load_mysql.unemployment import Unemployment
if __name__ == "__main__":
    try:
        covid = CovidData()
        covid.extract()
        covid_transform = covid_load()
        covid_transform.load_data()
        covid.create_country_dim()
        covid.create_normalized_facts()
    except Exception as ex:
        print(ex)
    try:
        housing = Housing()
        housing.load_dimensions()
        housing.create_normalized_fact()
    except Exception as ex:
        print(ex)
    try:
        stocks = Stocks()
        stocks.load_data()
    except Exception as ex:
        print(ex)
    try:
        stocks = Stocks()
        stocks.load_data()
    except Exception as ex:
        print(ex)
    try:
        unemployment = Unemployment()
        unemployment.load_data()
        unemployment.create_education_fact()
        unemployment.create_industry_fact()
        unemployment.create_race_fact()
        unemployment.load_state_unemployment_rate()
    except Exception as ex:
        print(ex)
