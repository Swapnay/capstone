from pandas_datareader import data
import pandas as pd
from toazureblob.sabutils import sablobutils
import yfinance as yf
import time
from datetime import date
import logging
from toazureblob.sourcetoblob.parent_source import ParentSource


class StockMarketData(ParentSource):
    S_P_500 = "https://datahub.io/core/s-and-p-500-companies/r/constituents.json"
    stocks_folder = "/Users/syeruvala/Downloads/archive/"
    symbols_valid = "Symbols_Valid_Meta"
    nassaq_url = "http://www.nasdaqtrader.com/dynamic/SymDir/nasdaqtraded.txt"
    symbols = list()
    logger = logging.getLogger('pyspark')

    def get_nasdaq_data(self):
        offset = 0
        limit = 0
        period = 'max'
        self.logger.info("get data from nassaq_url")
        data = pd.read_csv(self.nassaq_url, sep='|')
        data_clean = data[data['Test Issue'] == 'N']
        self.symbols = data_clean['NASDAQ Symbol'].tolist()
        print('total number of symbols traded = {}'.format(len(self.symbols)))
        limit = limit if limit else len(self.symbols)
        end = min(offset + limit, len(self.symbols))
        is_valid = [False] * len(self.symbols)
        for i in range(offset, end):
            try:
                s = self.symbols[i]
                try:
                    spark_df1 = sablobutils.read_spark_df_to_blob(s)
                    continue
                except Exception as ex:
                    self.logger.error("error %s", ex)
                self.logger.info("Download ticker from %s",s)
                data = yf.download(s, period=period)

                if len(data.index) == 0:
                    continue
                data.reset_index(inplace=True)
                data.columns =["Date","Open","High","Low","Close","AdjClose","Volume"]
                is_valid[i] = True
                sablobutils.write_to_blob(data, s)
            except Exception as ex:
                self.logger.error("error %s", ex)
                # hourly limit is 2000

        print('Total number of valid symbols downloaded = {}'.format(sum(is_valid)))

        valid_data = data_clean[is_valid]
        sablobutils.write_to_blob(valid_data, s)

    def get_s_p_stock_data(self):
        self.logger.debug("get data from StockMarketData.S_P_500")
        df = pd.read_json(StockMarketData.S_P_500)
        sablobutils.write_to_blob(df, "S_P_500")
        today = date.today()
        start_date = '2000-01-01'
        end_date = today.strftime("%Y-%m-%d")
        tickers = df["Symbol"].tolist()
        for ticker in tickers:
            self.logger.info("get data ticker {0}", ticker)
            if ticker in self.symbols:
                continue
            try:
                panel_data = data.DataReader(ticker, 'yahoo', start_date, end_date)
                panel_data.reset_index(inplace=True)
                panel_data.columns =["Date","Open","High","Low","Close","AdjClose","Volume"]
                sablobutils.write_to_blob(panel_data, ticker)
            except Exception as ex:
                self.logger.error("error %s", ex)
                continue

    def upload_to_azure_blob(self):
        self.get_nasdaq_data()
        self.get_s_p_stock_data()
