import pandas as pd


class StockMarketData:
    api_url = 'https://datahub.io/core/s-and-p-500-companies/r/constituents.json' '''Package('https://datahub.io/core/s-and-p-500-companies/datapackage.json')'''

    def extract(self):
        df = pd.read_csv('https://datahub.io/core/s-and-p-500-companies/r/constituents.csv')
        df.to_csv("../datasets/s_p_500.csv")


if __name__ == "__main__":
    stockMarketData = StockMarketData()
    stockMarketData.extract()
