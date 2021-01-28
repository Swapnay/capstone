
from sqlalchemy import text
import datetime
from pandas_datareader import data
import pandas as pd
from dbconfig import engine


class Stocks:
    insert_stock_dim = text("""INSERT INTO stocks_dim(symbol,name,category_name) VALUES(:symbol,:name,:category_name) ON DUPLICATE KEY UPDATE id=id""")
    select_stock_dim = """SELECT id FROM  stocks_dim WHERE symbol=%s"""
    insert_stock_fact = text("""INSERT INTO stock_prices_fact( stock_id, date_id, stock_date, open_price, closing_price, low, high, volume) 
    VALUES( :stock_id, :date_id, :stock_date, :open_price, :closing_price, :low, :high, :volume) ON DUPLICATE KEY UPDATE id=id""")
    date_dim_query = text("""INSERT INTO covid_date_dim(day, month, year) VALUES(:day,:month,:year) """)
    select_date_dim_query = """SELECT id from covid_date_dim where day=%s and month=%s and year=%s """

    def load_data(self):
        row_data = dict()
        sym_list = list()
        df = pd.read_csv("../datasets/s_p_500.csv")
        for i in range(0, df.shape[0], 5):
            json_list = list()
            with engine.connect() as conn:
                with conn.begin():
                    for j in range(5):
                        if (i + j) < df.shape[0]:
                            symbol = df.iloc[i + j]["Symbol"]
                            sym_list.append(symbol)
                            name = df.iloc[i + j]["Name"]
                            sector = df.iloc[i + j]["Sector"]
                            json_list.append({"symbol": symbol, "name": name, "category_name": sector})
                    if len(json_list) > 0:
                        data_save = tuple(json_list)
                        conn.execute(self.insert_stock_dim, data_save)

        for i in range(0, df.shape[0], 5):
            tickers = sym_list

            start_date = '2020-01-01'
            end_date = '2020-09-30'

            for ticker in tickers:
                try:
                    panel_data = data.DataReader(ticker, 'yahoo', start_date, end_date)
                except Exception as ex:
                    print(ex)
                    continue

                print(panel_data.shape[0])
                for i in range(0, panel_data.shape[0], 50):
                    json_list = list()
                    with engine.connect() as conn:
                        with conn.begin():
                            ticker_id = self.execute_query(conn, self.select_stock_dim, ticker)
                            try:
                                for j in range(50):

                                    if i + j < panel_data.shape[0]:
                                        print("i +j val"+str(i+j) )
                                        date = str(panel_data.iloc[i + j].name)
                                        high = panel_data.iloc[i + j]["High"]
                                        low = panel_data.iloc[i + j]["Low"]
                                        open = panel_data.iloc[i + j]["Open"]
                                        close = panel_data.iloc[i + j]["Close"]
                                        volume = panel_data.iloc[i + j]["Volume"]
                                        date_time_obj = datetime.datetime.strptime(date, '%Y-%m-%d %H:%M:%S')
                                        date_id = self.get_date_id(date_time_obj, conn)

                                        json_list.append({"stock_id": ticker_id, "date_id": date_id, "stock_date": date,
                                                          "open_price": open, "closing_price": close, "low": low, "high": high, "volume": volume})
                                if len(json_list) > 0:
                                    data_save = tuple(json_list)
                                    conn.execute(self.insert_stock_fact, data_save)
                            except Exception as e:
                                print(e)
                                continue



    def execute_query(self, conn, query, param):
        result = conn.execute(query, param)
        if result.rowcount > 0:
            row = result.fetchone()
            return row[0]

    def get_date_id(self, date_time_obj, conn):
        date_args = {"day": date_time_obj.day, "month": date_time_obj.month, "year": date_time_obj.year}
        date_id = self.execute_query(conn, self.select_date_dim_query, (date_time_obj.day, date_time_obj.month, date_time_obj.year))
        if date_id is None:
            result = conn.execute(self.date_dim_query, (date_args))
            date_id = result.lastrowid
        return date_id


if __name__ == "__main__":
    stocks = Stocks()
    stocks.load_data()
