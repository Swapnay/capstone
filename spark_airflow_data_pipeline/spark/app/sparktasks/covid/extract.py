import pandas as pd
from spark.app.sparktasks.utils.config import Config
import os
from pyspark.sql import SparkSession
from pyspark import SparkFiles
from spark.app.sparktasks.utils.DBUtils import DButils

class ExtractData:
    covid_config = {
        # "covid_us_data": "https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD",
        "covid_world_data": "https://covid.ourworldindata.org/data/owid-covid-data.csv"
    }

    def __init__(self):
        self.DButils = DButils()
        self.spark = SparkSession.builder.appName('Local').getOrCreate()
        self.config = Config()
        self.metadata_df = self.DButils.load_from_db(self.spark, self.config.get_config('GLOBAL', 'META_DATA_TABLE'))
        self.metadata_df.createGlobalTempView("metadata")
    #op_kwargs , use provide_context=True
    #dict_persons = {person['name']: person for person in list_persons}
    def get_last_exec_date(self,sector_type, **context):
        type_map = self.config.get_by_type(sector_type)
        print(type_map)
        length = len(type_map)
        type_keys = "','".join(dict(type_map).keys())
        types = '{}{}{}'.format("'",type_keys,"'")
        self.spark.read.table("global_temp.metadata")
        sql = 'SELECT * FROM global_temp.metadata WHERE TYPE IN ({}) ORDER BY execution_date DESC LIMIT {};'.format(types,length)
        spark_df = self.spark.sql(sql)
        df = spark_df.select('type','record_date')
        dict_val = map(lambda row: row.asDict(), df.collect())
        df_dict = [(r['type'], r['record_date']) for r in dict_val]
        #task_instance = context['task_instance']
        #task_instance.xcom_push(key=sector_type, value=dict_val)
        print(df_dict)
        self.metadata_dicts = dict(df_dict)


    def extract_covid19(self,sector_type,**kwargs):
        #ti = kwargs['ti']
        #msg = ti.xcom_pull(task_ids='push_task',key=sector_type)
        self.extract_from_source(self.config.covid19_source,self.metadata_dicts)

    def extract_housing(self,sector_type,**kwargs):
        #ti = kwargs['ti']
        #dict = ti.xcom_pull(task_ids='push_task',key=sector_type)
        housing_price = self.config.housing_price
        print(housing_price)
        self.extract_from_source(self.config.housing_price,self.metadata_dicts)
        housing_inventory = self.config.housing_inventory
        self.extract_from_source(self.config.housing_inventory,self.metadata_dicts)


    def extract_from_source(self, source_map, metadata_dicts): #column_name,
        print(source_map)
        housing_dict = dict(source_map)
        for key,value in housing_dict.items():
            covid_data = pd.read_csv(value)
            columns = covid_data.columns
            print(columns)
            #for j in range(8, covid_data.shape[1], 1):
              # print(columns[j])
            #covid_data[column_name] = pd.to_datetime(covid_data[column_name])
            #df = covid_data.loc[covid_data[column_name] > dict[key]]
            path = os.path.dirname(os.path.dirname(os.path.dirname(os.path.dirname(os.path.abspath(__file__)))))
            # covid_data.to_csv(path + "/resources/data/" + key + ".csv")
            columns=covid_data.columns
            record_date = metadata_dicts[key]
            if record_date is not None:
                date_time = record_date.strftime("%Y-%m-%d")
                index_to = columns.get_loc(date_time)
                index_from = columns.get_loc('1996-01-31')
                col_list = covid_data.columns[index_from:index_to+1].values.tolist()
                covid_data.drop(col_list, axis=1, inplace=True)
            #new_df = covid_data.drop(covid_data.ix[:,'1996-01-31':metadata_dicts[key]].head(0).columns, axis=1)
            path = path + "/resources/data/" + key + ".csv"
            covid_data.to_csv(path, index=False)

ExtractData =ExtractData()
ExtractData.get_last_exec_date('HOUSING_PRICE')
ExtractData.extract_housing('HOUSING_PRICE')
#ExtractData.extract_covid19('HOUSING_PRICE')
