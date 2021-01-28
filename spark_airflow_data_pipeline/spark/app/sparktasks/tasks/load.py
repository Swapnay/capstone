import sparktasks.blobutils.sablobutils as sablobutils
from os.path import join
import pyspark.sql.functions as func
from pyspark.sql.window import Window


class Load:


    def load_trade_to_blob(self,trade_common_df):
        spark = sablobutils.get_spark_session()
        #trade_common = spark.read.parquet("/output_dir/partition=T")
        trade = trade_common_df.select('trade_dt', 'symbol', 'exchange', 'event_time',
                                       'event_seq_num', 'arrival_time', 'trade_price',
                                       'trade_size')
        trade_filtered = self.applyLatest(trade)
        trade_date = trade_common_df.first().trade_dt.strftime('%Y-%m-%d')
        sablobutils.write_parquet_to_blob(trade_filtered,join("trade",trade_date))

    def load_quote_to_blob(self,quote_common):
        spark = sablobutils.get_spark_session()
        #quote_common = spark.read.parquet("/output_dir/partition=Q")

        quote = quote_common.select('trade_dt', 'symbol', 'exchange', 'event_time',
                                    'event_seq_num', 'arrival_time', 'bid_price',
                                    'bid_size','ask_price','ask_size')
        quote_filtered = self.applyLatest(quote)
        trade_date = quote_filtered.first().trade_dt.strftime('%Y-%m-%d')
        sablobutils.write_parquet_to_blob(quote_filtered,join("quote",trade_date))

    def applyLatest(self, trade):
        print(trade.show())
        pattern = "dd-MMM-yy hh.mm.ss.S"
        #trade.withColumn("event_time", func.unix_timestamp(trade.arrival_time, pattern).cast("timestamp").cast("long"))
        '''ranking=func.rank().over(
            Window.partitionBy("event_seq_num").orderBy(trade.event_time.desc())#.cast("timestamp").cast("long").desc)
        )'''
        #window_grp_order = Window.partitionBy('event_seq_num')#.orderBy(desc('arrival_time'))
        trade_filter = trade.withColumn("ranking",func.rank().over(
            Window.partitionBy("event_seq_num").orderBy(trade.arrival_time.desc())
        ))
        trade_new_rows = trade_filter.filter(trade_filter.ranking == '1')
        return trade_new_rows


#load = Load()
#load.load_trade_to_blob()
#load.load_quote_to_blob()