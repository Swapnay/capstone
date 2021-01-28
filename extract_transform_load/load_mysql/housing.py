import pandas as pd
from dbconfig import engine
from sqlalchemy import text
import numpy as np
import datetime
from extract_transform_load.extract.housing_data import price_config, inventory_metro_config


class Housing:
    value_dim_csv = "../datasets/housing_mid_tier.csv"
    sales_dim_csv = "../datasets/housing_for_sale.csv"
    date_dim_query = """INSERT INTO housing_date_dim (month, year) VALUES(%s,%s) RETURNING id"""
    '''ON DUPLICATE KEY UPDATE month=%s, year=%s '''
    city_dim_query = """INSERT INTO city_dim (city_name) VALUES(%s) RETURNING id""" '''ON DUPLICATE KEY UPDATE city_name=%s RETURNING id'''""
    metro_dim_query = """INSERT INTO metro_dim (metro_city_name) VALUES(%s) RETURNING id"""
    county_dim_query = """INSERT INTO county_dim (county_name) VALUES(%s) RETURNING id """
    select_date_dim_query = """SELECT id from housing_date_dim where month=%s and year=%s """
    select_city = """SELECT id FROM city_dim where city_name=%s"""
    select_metro = """SELECT id FROM metro_dim where metro_city_name=%s"""
    select_country = """SELECT id FROM  county_dim where county_name=%s"""
    select_state = """SELECT id FROM state_dim WHERE code =%s """
    insert_housing_city_fact = text(
        """INSERT INTO home_prices_fact( city_id, metro_id, date_id, inventory_date, state_id, county_id, mid_tier) VALUES(:city_id, :metro_id, :date_id,:inventory_date, :state_id, :county_id, :mid_tier )""")
    select_housing_city_fact = """SELECT id from home_prices_fact WHERE city_id =:city_id and date_id=:date_id AND state_id=:state_id AND county_id=:county_id"""
    update_housing_city_fact = text("""UPDATE home_prices_fact SET top_tier =:top_tier WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_bottom_housing_city_fact = text("""UPDATE home_prices_fact SET bottom_tier =:bottom_tier WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_single_housing_city_fact = text(
        """UPDATE home_prices_fact SET single_family =:single_family WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_condo_housing_city_fact = text("""UPDATE home_prices_fact SET condo =:condo WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_1bd_room_housing_city_fact = text("""UPDATE home_prices_fact SET 1bd_room =:1bd_room WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_2bd_room_housing_city_fact = text("""UPDATE home_prices_fact SET 2db_room =:2db_room WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_3bd_room_housing_city_fact = text("""UPDATE home_prices_fact SET 3bd_room =:3bd_room WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_4bd_room_housing_city_fact = text("""UPDATE home_prices_fact SET 4bd_room =:4bd_room WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    update_5bd_room_housing_city_fact = text("""UPDATE home_prices_fact SET 5bd_room =:5bd_room WHERE city_id =:city_id AND date_id =:date_id AND state_id=:state_id AND county_id=:county_id""")
    insert_housing_inventory_fact = text(
        """INSERT INTO home_inventory_sales_fact( metro_id, date_id, inventory_date, state_id, for_sale ) VALUES(:metro_id, :date_id, :inventory_date, :state_id, :for_sale)""")
    update_housing_inventory_median_days_fact = text(
        """UPDATE home_inventory_sales_fact SET median_days_to_sale_pending =:median_days_to_sale_pending WHERE  date_id =:date_id AND state_id=:state_id AND metro_id=:metro_id""")
    update_housing_inventory_median_sale_fact = text(
        """UPDATE home_inventory_sales_fact SET median_sale_price =:median_sale_price WHERE  date_id =:date_id AND state_id=:state_id AND metro_id=:metro_id""")

    def load_dimensions(self):
        for key in price_config:
            self.fill_dimensions("../datasets/" + key + ".csv", key)
        for key in inventory_metro_config:
            self.fill_dimensions("../datasets/" + key + ".csv", key)

    def fill_dimensions(self, file, type):
        df = pd.read_csv(file)
        columns = df.columns

        df = df.replace(np.nan, "", regex=True)


        for i in range(df.shape[0]):
            with engine.connect() as conn:
                state = df.iloc[i]["StateName"]
                region_name = df.iloc[i]["RegionName"]
                region_type = df.iloc[i]["RegionType"]
                if state == "":
                    continue

                if region_type == "City":
                    metro = df.iloc[i]["Metro"]
                    county_name = df.iloc[i]["CountyName"]
                    city_id = self.execute_query(conn, Housing.select_city, region_name)
                    if city_id is None:
                        city_id = self.execute_query(conn, Housing.city_dim_query, (region_name))
                    county_id = self.execute_query(conn, Housing.select_country, county_name)
                    if county_id is None:
                        county_id = self.execute_query(conn, Housing.county_dim_query, (county_name))
                else:
                    metro = region_name
                metro_id = self.execute_query(conn, Housing.select_metro, metro)
                if metro_id is None:
                    metro_id = self.execute_query(conn, Housing.metro_dim_query, (metro))
                state_id = self.execute_query(conn, Housing.select_state, (state))

                for j in range(9, df.shape[1], 1):
                    date = columns[j]
                    date_time_obj = datetime.datetime.strptime(date, '%Y-%m-%d')
                    date_args = (date_time_obj.month, date_time_obj.year, date_time_obj.month, date_time_obj.year)
                    date_id = self.execute_query(conn, Housing.select_date_dim_query, (date_time_obj.month, date_time_obj.year))
                    if date_id is None:
                        date_id = self.execute_query(conn, Housing.date_dim_query, date_args)
                    if df.iloc[i][j] == "":
                        continue
                    if region_type == "City":

                        if type == "housing_mid_tier":
                            mid_tier = df.iloc[i][j]

                            args = {"city_id": city_id, "metro_id": metro_id, "date_id": date_id, "inventory_date": date_time_obj, "state_id": state_id, "county_id": county_id, "mid_tier": mid_tier}
                            conn.execute(Housing.insert_housing_city_fact, (args))
                        elif type == "housing_top_tier":
                            top_tier = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "top_tier": top_tier}
                            conn.execute(Housing.update_housing_city_fact, args)
                        elif type == "housing_bottom_tier":
                            bottom_tier = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "bottom_tier": bottom_tier}
                            conn.execute(Housing.update_bottom_housing_city_fact, args)
                        elif type == "housing_single_family":
                            single_tier = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "single_family": single_tier}
                            conn.execute(Housing.update_single_housing_city_fact, args)
                        elif type == "housing_condo":
                            condo = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "condo": condo}
                            conn.execute(Housing.update_condo_housing_city_fact, args)
                        elif type == "housing_1_bd":
                            bd_room = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "1bd_room": bd_room}
                            conn.execute(Housing.update_1bd_room_housing_city_fact, args)
                        elif type == "housing_2_bd":
                            bd_room = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "2db_room": bd_room}
                            conn.execute(Housing.update_2bd_room_housing_city_fact, args)
                        elif type == "housing_3_bd":
                            bd_room = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "3bd_room": bd_room}
                            conn.execute(Housing.update_3bd_room_housing_city_fact, args)
                        elif type == "housing_4_bd":
                            bd_room = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "4bd_room": bd_room}
                            conn.execute(Housing.update_4bd_room_housing_city_fact, args)
                        elif type == "housing_5_bd":
                            bd_room = df.iloc[i][j]
                            args = {"city_id": city_id, "date_id": date_id, "state_id": state_id, "county_id": county_id, "5bd_room": bd_room}
                            conn.execute(Housing.update_5bd_room_housing_city_fact, args)
                    else:
                        if type == "housing_for_sale":
                            for_sale = df.iloc[i][j]
                            args = {"metro_id": metro_id, "date_id": date_id, "inventory_date": date_time_obj, "state_id": state_id, "for_sale": for_sale}
                            conn.execute(Housing.insert_housing_inventory_fact, args)
                        elif type == "housing_days_to_pending":
                            for_sale = df.iloc[i][j]
                            args = {"metro_id": metro_id, "date_id": date_id, "inventory_date": date_time_obj, "state_id": state_id, "median_days_to_sale_pending": for_sale}
                            conn.execute(Housing.update_housing_inventory_median_days_fact, args)
                        elif type == "housing_median_sale_price":
                            for_sale = df.iloc[i][j]
                            args = {"metro_id": metro_id, "date_id": date_id, "inventory_date": date_time_obj, "state_id": state_id, "median_sale_price": for_sale}
                            conn.execute(Housing.update_housing_inventory_median_sale_fact, args)

    def execute_query(self, conn, query, param):
        result = conn.execute(query, param)

        if result.rowcount > 0:
            row = result.fetchone()
            return row[0]


    def create_normalized_fact(self):
        df = pd.read_sql("""
            SELECT  city_id, metro_id, date_id, inventory_date, state_id, county_id, mid_tier, top_tier, bottom_tier, single_family, condo,
            1bd_room, 2db_room, 3bd_room, 4bd_room, 5bd_room
            FROM home_prices_fact
            ORDER BY id
            """, con=engine,index_col=None,chunksize = 1000)

        for dat_frame in df:
            insert_list = []
            for i in range(dat_frame.shape[0]):
                insert_val = {}
                insert_val["city_id"] = dat_frame.iloc[i][0]
                insert_val["metro_id"] = dat_frame.iloc[i][1]
                insert_val["date_id"] = dat_frame.iloc[i][2]
                insert_val["inventory_date"] = dat_frame.iloc[i][3]
                insert_val["state_id"] =dat_frame.iloc[i][4]
                insert_val["county_id"] =dat_frame.iloc[i][5]
                insert_val["inventory_type"] = "mid_tier"
                insert_val["price"] = dat_frame.iloc[i][6]
                insert_list.append(insert_val)

                insert_val1 = {}
                insert_val1["city_id"] = dat_frame.iloc[i][0]
                insert_val1["metro_id"] = dat_frame.iloc[i][1]
                insert_val1["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val1["inventory_date"] = dat_frame.iloc[i][3]
                insert_val1["state_id"] =dat_frame.iloc[i][4]
                insert_val1["county_id"] =dat_frame.iloc[i][5]
                insert_val1["inventory_type"] = "top_tier"
                insert_val1["price"] = dat_frame.iloc[i][7]
                insert_list.append(insert_val1)

                insert_val2 = {}
                insert_val2["city_id"] = dat_frame.iloc[i][0]
                insert_val2["metro_id"] = dat_frame.iloc[i][1]
                insert_val2["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val2["inventory_date"] = dat_frame.iloc[i][3]
                insert_val2["state_id"] =dat_frame.iloc[i][4]
                insert_val2["county_id"] =dat_frame.iloc[i][5]
                insert_val2["inventory_type"] = "bottom_tier"
                insert_val2["price"] = dat_frame.iloc[i][8]
                insert_list.append(insert_val2)

                insert_val3 = {}
                insert_val3["city_id"] = dat_frame.iloc[i][0]
                insert_val3["metro_id"] = dat_frame.iloc[i][1]
                insert_val3["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val3["inventory_date"] = dat_frame.iloc[i][3]
                insert_val3["state_id"] =dat_frame.iloc[i][4]
                insert_val3["county_id"] =dat_frame.iloc[i][5]
                insert_val3["inventory_type"] = "single_family"
                insert_val3["price"] = dat_frame.iloc[i][9]
                insert_list.append(insert_val3)

                insert_val4 = {}
                insert_val4["city_id"] = dat_frame.iloc[i][0]
                insert_val4["metro_id"] = dat_frame.iloc[i][1]
                insert_val4["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val4["inventory_date"] = dat_frame.iloc[i][3]
                insert_val4["state_id"] =dat_frame.iloc[i][4]
                insert_val4["county_id"] =dat_frame.iloc[i][5]
                insert_val4["inventory_type"] = "condo"
                insert_val4["price"] = dat_frame.iloc[i][10]
                insert_list.append(insert_val4)

                insert_val5 = {}
                insert_val5["city_id"] = dat_frame.iloc[i][0]
                insert_val5["metro_id"] = dat_frame.iloc[i][1]
                insert_val5["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val5["inventory_date"] = dat_frame.iloc[i][3]
                insert_val5["state_id"] =dat_frame.iloc[i][4]
                insert_val5["county_id"] =dat_frame.iloc[i][5]
                insert_val5["inventory_type"] = "1bd"
                insert_val5["price"] = dat_frame.iloc[i][11]
                insert_list.append(insert_val5)

                insert_val6 = {}
                insert_val6["city_id"] = dat_frame.iloc[i][0]
                insert_val6["metro_id"] = dat_frame.iloc[i][1]
                insert_val6["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val6["inventory_date"] = dat_frame.iloc[i][3]
                insert_val6["state_id"] =dat_frame.iloc[i][4]
                insert_val6["county_id"] =dat_frame.iloc[i][5]
                insert_val6["inventory_type"] = "2bd"
                insert_val6["price"] = dat_frame.iloc[i][12]
                insert_list.append(insert_val6)

                insert_val7 = {}
                insert_val7["city_id"] = dat_frame.iloc[i][0]
                insert_val7["metro_id"] = dat_frame.iloc[i][1]
                insert_val7["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val7["inventory_date"] = dat_frame.iloc[i][3]
                insert_val7["state_id"] =dat_frame.iloc[i][4]
                insert_val7["county_id"] =dat_frame.iloc[i][5]
                insert_val7["inventory_type"] = "3bd"
                insert_val7["price"] = dat_frame.iloc[i][13]
                insert_list.append(insert_val7)

                insert_val8 = {}
                insert_val8["city_id"] = dat_frame.iloc[i][0]
                insert_val8["metro_id"] = dat_frame.iloc[i][1]
                insert_val8["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val8["inventory_date"] = dat_frame.iloc[i][3]
                insert_val8["state_id"] =dat_frame.iloc[i][4]
                insert_val8["county_id"] =dat_frame.iloc[i][5]
                insert_val8["inventory_type"] = "4bd"
                insert_val8["price"] = dat_frame.iloc[i][14]
                insert_list.append(insert_val8)

                insert_val9 = {}
                insert_val9["city_id"] = dat_frame.iloc[i][0]
                insert_val9["metro_id"] = dat_frame.iloc[i][1]
                insert_val9["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val9["inventory_date"] = dat_frame.iloc[i][3]
                insert_val9["state_id"] =dat_frame.iloc[i][4]
                insert_val9["county_id"] =dat_frame.iloc[i][5]
                insert_val9["inventory_type"] = "5bd"
                insert_val9["price"] = dat_frame.iloc[i][15]
                insert_list.append(insert_val9)
            data_fr = pd.DataFrame(insert_list)
            data_fr.drop_duplicates(subset=['city_id', 'metro_id', 'date_id', 'state_id', 'county_id','inventory_type'], keep='first', inplace=True, ignore_index=False)
            data_fr.to_sql('home_prices_normalized_fact', con = engine, if_exists = 'append',chunksize = 1000, index= False)




if __name__ == "__main__":
    housing = Housing()
   # housing.load_dimensions()
    housing.create_normalized_fact()
