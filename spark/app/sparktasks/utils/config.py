import configparser
import os


class Config:
    config = configparser.RawConfigParser()
    imperial_path =os.getenv('SPARK_HOME')
    config.read_file(open(os.path.join(imperial_path, "app", "sparktasks", "config.cfg")))

    @property
    def mysql_user(self):
        return Config.config.get('MYSQL', 'USER')

    @property
    def mysql_password(self):
        return self.config.get('MYSQL', 'PASSWORD')

    @property
    def mysql_host(self):
        return self.config.get('MYSQL', 'HOST')

    @property
    def mysql_port(self):
        return self.config.get('MYSQL', 'PORT')

    @property
    def mysql_db_name(self):
        return self.config.get('MYSQL', 'DATABASE')

    @property
    def driver_name(self):
        return self.config.get('MYSQL', 'DRIVER')

    @property
    def connection_str_jdbc(self):
        return 'jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true&serverTimezone=UTC'.format(self.mysql_host, self.mysql_port, self.mysql_db_name)

    @property
    def covid19_source(self):
        return self.config.items('COVID')

    @property
    def housing_price(self):
        return self.config.items('HOUSING_PRICE')

    @property
    def housing_inventory(self):
        return self.config.items('HOUSING_INVENTORY')

    @property
    def employment(self):
        return self.config.items('UNEMPLOYMENT')

    @property
    def stocks_sp(self):
        return self.config.get('STOCKS','STOCKS_SP_500')

    @property
    def stocks_start_date(self):
        return self.config.get('STOCKS','STOCKS_START_DATE')

    @property
    def state_dim(self):
        return self.config.get('DIMS', 'STATE_DIM')

    @property
    def date_dim(self):
        return self.config.get('DIMS', 'DATE_DIM')

    @property
    def housing_date_dim(self):
        return self.config.get('DIMS', 'HOUSING_DATE_DIM')

    @property
    def city_dim(self):
        return self.config.get('DIMS', 'CITY_DIM')

    @property
    def county_dim(self):
        return self.config.get('DIMS', 'COUNTY_DIM')

    @property
    def metro_dim(self):
        return self.config.get('DIMS', 'METRO_DIM')

    @property
    def unemployment_series_dim(self):
        return self.config.get('DIMS', 'UNEMPLOYMENT_SERIES_DIM')

    @property
    def stocks_dim(self):
        return self.config.get('DIMS', 'STOCKS_DIM')

    @property
    def country_details_dim(self):
        return self.config.get('DIMS', 'COUNTRY_DETAILS_DIM')

    @property
    def covid_world_raw(self):
        return self.config.get('RAW', 'COVID_WORLD_DATA')

    @property
    def covid_world_sector(self):
        return 'COVID_WORLD_DATA'.lower()

    @property
    def covid_usa_raw(self):
        return self.config.get('RAW', 'COVID_US_DATA')

    @property
    def covid_world_fact(self):
        return self.config.get('FACTS', 'COVID_WORLD_DATA')

    @property
    def housing_inventory_fact(self):
        return self.config.get('FACTS', 'HOUSING_INVENTORY')


    @property
    def housing_price_fact(self):
        return self.config.get('FACTS', 'HOUSING_PRICE')

    @property
    def housing_inventory_monthly(self):
        return self.config.get('ANALYTICS', 'HOUSING_INVENTORY')


    @property
    def housing_price_monthly(self):
        return self.config.get('ANALYTICS', 'HOUSING_PRICE')

    @property
    def covid_usa_sector(self):
        return 'COVID_US_DATA'.lower()

    @property
    def covid_usa_fact(self):
        return self.config.get('FACTS', 'COVID_US_DATA')

    @property
    def covid_world_analytics(self):
        return self.config.get('ANALYTICS', 'COVID_WORLD')

    @property
    def covid_usa_analytics(self):
        return self.config.get('ANALYTICS', 'COVID_USA')

    @property
    def stocks_raw(self):
        return self.config.get('RAW', 'STOCKS')

    @property
    def stocks_fact(self):
        return self.config.get('FACTS', 'STOCKS')

    @property
    def stocks_analytics(self):
        return self.config.get('ANALYTICS', 'STOCKS')

    @property
    def unemployment_analytics(self):
        return self.config.get('ANALYTICS', 'UNEMPLOYMENT')

    @property
    def metadata(self):
        return self.config.get('GLOBAL', 'META_DATA_TABLE')

    @property
    def data_dir(self):
        return os.path.join(self.imperial_path, "resources", "data")

    def get_by_type(self, sector_type):
        return self.config.items(sector_type)

    def get_raw_by_sector(self, sector_name):
        return self.config.get('RAW', sector_name)

    def get_config(self, group, key):
        return self.config.get(group, key)

    def set_config(self, group, key, value):
        group = self.config[group]
        group[key] = value
        # Write changes back to file
        with open("/sparktasks/config.cfg", 'w') as conf:
            self.config.write(conf)
