import configparser
from pathlib import Path
import os
import logging


class Config:
    config = configparser.RawConfigParser()
    config.read_file(open("../config.cfg"))

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
        return 'jdbc:mysql://{}:{}/{}?useUnicode=true&characterEncoding=UTF-8&rewriteBatchedStatements=true'.format(self.mysql_host, self.mysql_port, self.mysql_db_name)

    @property
    def covid19_source(self):
        return self.config.items('COVID')

    @property
    def housing_price(self):
        return self.config.items('HOUSING_PRICE')

    @property
    def housing_inventory(self):
        return self.config.items('HOUSING_INVENTORY')


    def get_by_type(self,sector_type):
        return self.config.items(sector_type)

    def get_config(self,group,key):
        return self.config.get(group,key)

    def set_config(self, group,key,value):
        group = self.config[group]
        group[key] = value
        #Write changes back to file
        with open("../config.cfg", 'w') as conf:
            self.config.write(conf)




