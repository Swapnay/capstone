'''from yahoo_finance import Share
from pprint import pprint

yahoo = Share('YHOO')
pprint(yahoo.get_historical('2014-04-25', '2014-04-29'))
from pandas_datareader import data
import pandas as pd

tickers = ['AAPL', 'MSFT', '^GSPC']

# We would like all available data from 01/01/2000 until 12/31/2016.
start_date = '2010-01-01'
end_date = '2016-12-31'

# User pandas_reader.data.DataReader to load the desired data. As simple as that.
panel_data = data.DataReader('INPX', 'google', start_date, end_date)
panel_data.to_frame().head(8)'''
from iexfinance.refdata import get_symbols
from iexfinance.stocks import Stock
from datetime import datetime
from iexfinance.stocks import get_historical_data

get_symbols(output_format='pandas', token="<TOKEN>")


a = Stock("AAPL", token="<TOKEN>")


start = datetime(2016, 1, 1)
end = datetime(2019, 7, 30)
print(get_historical_data("AAPL", start, end,
                           output_format='pandas',
                               token='<TOKEN>'))


'''import pandas as pd
from iexfinance.stocks import Stock
from datetime import datetime

from iexfinance.stocks import get_historical_data
def getCompanyInfo(symbols):
    stock_batch = Stock(symbols,
                        token='<TOKEN>')
    company_info = stock_batch.get_company()
    return company_info
def getEarnings(symbol):
    stock_batch = Stock(symbol,
                        token='<TOKEN>')
    earnings = stock_batch.get_earnings(last=4)
    return earnings
def getHistoricalPrices(stock):
    start = datetime(2016, 1, 1)
    end = datetime(2019, 7, 30)
    return get_historical_data(stock, start, end,
                               output_format='pandas',
                               token='<TOKEN>')
sp = pd.read_csv('S&P500-Symbols.csv', index_col=[0])
sp_company_info = getCompanyInfo(sp["Symbol"][:5].tolist())
company_info_to_df = []
for company in sp_company_info:
    company_info_to_df.append(sp_company_info[company])
columns = ['symbol', 'companyName', 'exchange',
           'industry', 'website', 'CEO', 'sector']
df = pd.DataFrame(company_info_to_df, columns=columns )
print(df.head())
single_stock_earnings = getEarnings(sp["Symbol"][0])
df_earnings = pd.DataFrame(single_stock_earnings)
df_earnings.head()

single_stock_history = getHistoricalPrices(sp["Symbol"][0])
#single_stock_history['close'].plot(label="3M Close")
print(single_stock_history)'''