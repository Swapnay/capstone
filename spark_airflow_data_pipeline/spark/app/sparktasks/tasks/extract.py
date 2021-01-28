from sparktasks.blobutils import sablobutils
from sparktasks.blobutils import parseutils
from pyspark.sql import SparkSession
from sparktasks.tasks.load import Load
from pyspark import SparkConf, SparkContext


class Extract:
    __csv_file_list = ["quote.csv", "trade.csv"]
    __json_file_list = ["quotejson.json", "trade.csv", "tradejson.json"]

    def extract_csv(self):
        spark =  SparkSession.builder.appName('Extract').getOrCreate()
        for file in self.__csv_file_list:
            raw = sablobutils.read_raw_text_from_blob(spark,file)
        parsed = raw.map(lambda line: parseutils.parse_csv(line))
        data = spark.createDataFrame(parsed)
        print(data.show())
        self.load_quote_trade(data)

    def load_quote_trade(self,data):
        spark_df = data.filter("trade_dt is not NULL")
        df_quote = spark_df.filter("partition='Q'")
        df_trade = spark_df.filter("partition='T'")
        load = Load()
        if len(df_quote.head(1)) != 0 :
            load.load_quote_to_blob(df_quote)
        if len(df_trade.head(1)) != 0 :
            load.load_trade_to_blob(df_trade)

    def extract_json(self):
        spark = SparkSession.builder.appName('Extract').getOrCreate()
        for file in self.__json_file_list:
            raw = sablobutils.read_raw_text_from_blob(spark,file)
        parsed = raw.map(lambda line: parseutils.parse_json(line))
        data = spark.createDataFrame(parsed)
        print(data.show())
        self.load_quote_trade(data)


'''extract = Extract()

extract.extract_csv(join(Path().resolve().parent,"data/trade.csv"))
extract.extract_csv(join(Path().resolve().parent,"data/quote.csv"))
extract.extract_json(join(Path().resolve().parent,"data/tradejson.json"))
extract.extract_json(join(Path().resolve().parent,"data/quotejson.json"))'''

extract = Extract()
extract.extract_json()
extract.extract_csv()
