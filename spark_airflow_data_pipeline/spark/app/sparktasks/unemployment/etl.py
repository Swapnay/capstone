from spark.app.sparktasks.unemployment.extract import Extract
from spark.app.sparktasks.unemployment.transform_load import TransformLoad

class ETL:

    @staticmethod
    def extract():
        extract = Extract()
        extract.extract_from_source()
        extract.store_raw_in_db()

    @staticmethod
    def transform_load():
        transform_load = TransformLoad()
        transform_load.transform_load_data()

if __name__ == "__main__":
    ETL.extract()
    ETL.transform_load()
    #for i,j in zip(range(591,640), range(1,32) ):
       # print("insert into covid_date_dim values({},{},8,2021);".format(i,j))

