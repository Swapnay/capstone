import pandas as pd
from dbconfig import engine
import prettytable
from sqlalchemy import text
import matplotlib
matplotlib.use("PDF")
import matplotlib.pyplot as plt


class eda:
    covid_monthly_change_rate ='''WITH monthly_closing_covid AS(                            
                            SELECT  c.country_name AS country,d.month AS month,d.year AS year,avg(new_cases_per_million) as avg_new_cases,last_value(cd.total_cases) 
                            over(partition BY d.month,d.year,c.country_name ORDER BY c.country_name) AS monthly
                             FROM covid_world_normalized_fact cd
                             INNER JOIN covid_economy_impact.country_dim c ON cd.country_id = c.id
                             INNER JOIN covid_economy_impact.covid_date_dim d on d.id = cd.date_id GROUP BY country,month)
                            SELECT * FROM(    
                                  SELECT a.country,a.year, a.month,100*(a.monthly- b.monthly)/b.monthly AS monthly_rate,a.avg_new_cases,
                                        100*(a.avg_new_cases- b.avg_new_cases)/b.avg_new_cases AS increase_change_rate,a.monthly AS closing_cases 
                                    FROM monthly_closing_covid a 
                                    INNER JOIN monthly_closing_covid b
                                    ON a.country = b.country AND a.month= b.month+1  
                                    ORDER BY a.year, a.month) AS avg_cases_tbl
                                    {0};'''
    covid_query = '''SELECT country,month,avg_new_cases,total_cases_per_million 
                     FROM covid_monthly_avg_table where month ={0} and ranking<={1} order by avg_new_cases {2};'''

    stock_cte = '''WITH monthly_closing_stock AS (
                        SELECT DISTINCT * FROM(
                            SELECT sp.stock_id,s.symbol,d.month,d.year, s.category_name, last_value(sp.closing_price) 
                                OVER(partition BY  d.month,sp.stock_id ORDER BY sp.stock_id, month) AS CLOSE
                        FROM stock_prices_fact sp 
                            INNER JOIN covid_date_dim d ON d.id = sp.date_id
                            INNER JOIN stocks_dim s ON s.id=sp.stock_id ) AS closing_val)
                        SELECT a.symbol,a.category_name,a.Year as  year, a.Month as month,100*(a.close- b.close)/b.close AS return_rate,
                                a.close AS cosing_price 
                        FROM monthly_closing_stock a 
                            INNER JOIN monthly_closing_stock b
                            ON a.stock_id = b.stock_id AND a.month= b.month+1 '''
    stock_query ='''SELECT * FROM ( SELECT m.symbol,m.category_name,m.year,avg(return_rate) AS avg_monthly_return,
                               rank() over(partition by m.category_name ORDER BY AVG(return_rate) {0}) AS rank1
                        FROM stocks_monthly_avg_return m 
                        GROUP BY m.symbol,m.category_name) AS yearly_rate
                    WHERE rank1<={1};'''
    stock_orderly ='''SELECT m.symbol,m.category_name,m.year,avg(m.return_rate) AS avg_monthly_return 
                        FROM stocks_monthly_avg_return m 
                         GROUP BY m.symbol,m.year,m.category_name 
                         order by avg_monthly_return {0} {1};'''

    def covid_monthly_rate(self,country='United States'):
        query = self.covid_monthly_change_rate.format(" WHERE country ='" +country +"' AND year=2020")
        df = pd.read_sql(query, con=engine, index_col=None)
        self.format_for_print(df)

    def draw_line_plot(self):

        pass


    def covid_effected(self, month, order="", count=10):
        query = str(self.covid_query).format( month, count,order)
        df = pd.read_sql(query, con=engine, index_col=None)
        self.format_for_print(df)

    def format_for_print(self, df):
        table = prettytable.PrettyTable([''] + list(df.columns))
        for row in df.itertuples():
            table.add_row(row)
        return print(table)

    def stock_price_by_category(self,order="",count=1):
        query = self.stock_query.format(order,count)
        df = pd.read_sql(query, con=engine, index_col=None)
        self.format_for_print(df)

    def stock_order_by(self,order="desc",count=10):
        query = self.stock_orderly.format(order, "limit "+str(count) )
        df = pd.read_sql(query, con=engine, index_col=None)
        self.format_for_print(df)

    def stock_price_monthly(self):
        query = self.stock_cte
        df = pd.read_sql(query, con=engine, index_col=None)
        df.to_sql('stocks_monthly_avg_return', con = engine, if_exists = 'append', index= id)
        self.format_for_print(df)



if __name__ == "__main__":
    eda = eda()

    '''try:
        eda.stock_price_monthly()
    except Exception as ex:
        print(ex)'''
    eda.covid_monthly_rate()
    eda.stock_order_by("",17)
    print("most effected countries with average number per million  at beginning and recent")
    #eda.covid_effected(3, "desc")
    #eda.covid_effected(10, "desc")
    print("least effected. Average number of cases per million  countries at beginning and recent")
    #eda.covid_effected(3, "")
    #eda.covid_effected(10, "")
    #GE,CCL,COTY,OXY,HFC,AAL fell
    #AMZN,AAPL,FDX,NVDA,UPS,WST,DXCM,EBAY
