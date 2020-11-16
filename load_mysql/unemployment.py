from extract.unemploymentdata import UnemploymentData
import json
from sqlalchemy import text
import pandas as pd
from dbconfig import engine
import datetime

class Unemployment:
    series_column={
        "LNU00000003":"civilian_noninstitutional_white",
        "LNU01000003": "white_participated",
        "LNU01300003":"white_participated_rate",
        "LNU03000003":"white_unemployed",
        "LNU04000003":"white_unemployed_rate",
        "LNU00000006":"civilian_noninstitutional_black",
        "LNU01000006":"black_participated",
        "LNU01300006":"black_participated_rate",
        "LNU03000006":"black_unemployed",
        "LNU04000006":"black_unemployed_rate",
        "LNU00032183":"civilian_noninstitutional_hlo",
        "LNU01032183":"hlo_participated",
        "LNU01332183":"hlo_participated_rate",
        "LNU03032183":"hlo_unemployed",
        "LNU04032183":"hlo_unemployed_rate",
        "LNU00000009":"civilian_noninstitutional_asian",
        "LNU01000009":"asian_participated",
        "LNU01300009":"asian_participated_rate",
        "LNU03000009":"asian_unemployed",
        "LNU04000009":"asian_unemployed_rate",

        "LNU01027659": "less_than_high_school_participated",
        "LNU01327659": "less_than_high_school_participated_rate",
        "LNU03027659": "less_than_high_school_unemployed",
        "LNU04027659": "less_than_high_school_unemployed_rate",
        "LNU01027660" : "high_school_grad_participated",
        "LNU01327660": "high_school_grad_participated_rate",
        "LNU03027660": "high_school_grad_unemployed",
        "LNU04027660":"high_school_grad_unemployed_rate",
        "LNU01027689":"some_college_associate_participated",
        "LNU01327689":"some_college_associate_participated_rate",
        "LNU03027689" : "some_college_associate_unemployed" ,
        "LNU04027689": "some_college_associate_unemployed_rate",
        "LNU01027662": "bachelors_or_higher_college_associate_participated",
        "LNU01327662": "bachelors_or_higher_college_associate_participated_rate",
        "LNU03027662": "bachelors_or_higher_college_associate_unemployed",
        "LNU04027662": "bachelors_or_higher_college_associate_unemployed_rate",

        "LNU03000000":"total_unemployment",
        "LNU04000000":"unemployment_rate",
        "LNU03032231":"construction",
        "LNU04032231": "construction_rate",
        "LNU03032232": "manufacturing",
        "LNU04032232": "manufacturing_rate",
        "LNU03032235": "whole_sale_trade",
        "LNU04032235": "whole_sale_trade_rate",
        "LNU03032236": "transportation_and_utilities",
        "LNU04032236": "transportation_and_utilities_rate",
        "LNU03032239": "professional_business",
        "LNU04032239": "professional_business_rate",
        "LNU03032240": "education_health_care",
        "LNU04032240": "education_health_care_rate",
        "LNU03032241":"leisure_hospitality",
        "LNU04032241": "leisure_hospitality_rate",
        "LNU03035109": "agriculture_related",
        "LNU04035109": "agriculture_related_rate"


    }

    query ={
        "by_race":text("""INSERT INTO unemployment_by_race_fact(date_id, civilian_noninstitutional_white, white_participated, white_participated_rate, white_unemployed, white_unemployed_rate, 
        civilian_noninstitutional_black, black_participated, black_participated_rate, black_unemployed, black_unemployed_rate, civilian_noninstitutional_hlo, hlo_participated, hlo_participated_rate, hlo_unemployed, hlo_unemployed_rate, civilian_noninstitutional_asian, asian_participated, asian_participated_rate, asian_unemployed, asian_unemployed_rate,submission_date) 
        VALUES(:date_id, :civilian_noninstitutional_white, :white_participated, :white_participated_rate, :white_unemployed, :white_unemployed_rate, :civilian_noninstitutional_black, :black_participated, :black_participated_rate, :black_unemployed, :black_unemployed_rate, :civilian_noninstitutional_hlo, :hlo_participated, :hlo_participated_rate, :hlo_unemployed, :hlo_unemployed_rate, :civilian_noninstitutional_asian, :asian_participated, :asian_participated_rate, :asian_unemployed, :asian_unemployed_rate,:submission_date)"""),
        "by_education_level":text("""INSERT INTO unemployment_by_education_level_fact( date_id, less_than_high_school_participated, less_than_high_school_participated_rate, less_than_high_school_unemployed, less_than_high_school_unemployed_rate, high_school_grad_participated, high_school_grad_participated_rate, high_school_grad_unemployed, high_school_grad_unemployed_rate, some_college_associate_participated, some_college_associate_participated_rate, some_college_associate_unemployed, some_college_associate_unemployed_rate, bachelors_or_higher_college_associate_participated, bachelors_or_higher_college_associate_participated_rate, bachelors_or_higher_college_associate_unemployed, bachelors_or_higher_college_associate_unemployed_rate,submission_date) 
        VALUES( :date_id, :less_than_high_school_participated, :less_than_high_school_participated_rate, :less_than_high_school_unemployed, :less_than_high_school_unemployed_rate, :high_school_grad_participated, :high_school_grad_participated_rate, :high_school_grad_unemployed, :high_school_grad_unemployed_rate, :some_college_associate_participated, :some_college_associate_participated_rate, :some_college_associate_unemployed, :some_college_associate_unemployed_rate, :bachelors_or_higher_college_associate_participated, :bachelors_or_higher_college_associate_participated_rate, :bachelors_or_higher_college_associate_unemployed, :bachelors_or_higher_college_associate_unemployed_rate,:submission_date) """) ,
        "by_industry": text("""INSERT into unemployment_by_industry_fact( date_id, total_unemployment, unemployment_rate, construction, construction_rate, manufacturing, manufacturing_rate, whole_sale_trade, whole_sale_trade_rate, transportation_and_utilities, transportation_and_utilities_rate, professional_business, professional_business_rate, education_health_care, education_health_care_rate, leisure_hospitality, leisure_hospitality_rate, agriculture_related, agriculture_related_rate,submission_date)
         VALUES( :date_id, :total_unemployment, :unemployment_rate, :construction, :construction_rate, :manufacturing, :manufacturing_rate, :whole_sale_trade, :whole_sale_trade_rate, :transportation_and_utilities, :transportation_and_utilities_rate, :professional_business, :professional_business_rate, :education_health_care, :education_health_care_rate, :leisure_hospitality, :leisure_hospitality_rate, :agriculture_related, :agriculture_related_rate,:submission_date)""")
    }
    select_date_dim_query = """SELECT id from housing_date_dim where month=%s and year=%s """



    def load_data(self):
        for key in UnemploymentData.series:

            with open("../datasets/"+key+".json") as f:
                json_data = json.load(f)

            for i in range(len(json_data['Results']['series'][0]["data"])):
                row_data = dict()
                with engine.connect() as conn:
                    for j in range(len(json_data['Results']['series'])):
                        series_id =json_data['Results']['series'][j]['seriesID']
                        year = int(json_data['Results']['series'][j]["data"][i]["year"])
                        period = str(json_data['Results']['series'][j]["data"][i]['period'])
                        value = json_data['Results']['series'][j]["data"][i]['value']
                        split = period.split("M")
                        month = int(split[1])
                        row_data[self.series_column.get(series_id)] = value
                        result = conn.execute(self.select_date_dim_query,(month,year))
                        if result.rowcount>0:
                            row = result.fetchone()
                            date_id=row[0]
                        row_data["date_id"] = date_id
                        date = datetime.datetime(year,month,1)
                        row_data["submission_date"] = date


                    '''print("series_id"+series_id +""+value)
                    print(row_data)
                    print(self.query.get(key))
                    print( json.dumps(row_data))'''
                    conn.execute(self.query.get(key), row_data)




    def create_normalized_fact(self):
        df = pd.read_sql("""
            SELECT date_id, civilian_noninstitutional_white, white_participated, white_participated_rate, white_unemployed, white_unemployed_rate, 
            civilian_noninstitutional_black, black_participated, black_participated_rate, black_unemployed, black_unemployed_rate, civilian_noninstitutional_hlo,
            hlo_participated, hlo_participated_rate, hlo_unemployed, hlo_unemployed_rate, civilian_noninstitutional_asian, asian_participated, asian_participated_rate, 
            asian_unemployed, asian_unemployed_rate,submission_date
            FROM unemployment_by_race_fact
            ORDER BY id
            """, con=engine,index_col=None,chunksize = 1000)


        column_names = ["date_id", "race_type", "civilian_noninstitutional","participated","unemployed","unemployed_rate"]
        data = pd.DataFrame(columns = column_names)

        for dat_frame in df:
            insert_list = []
            try:
                for i in range(dat_frame.shape[0]):
                    insert_val = {}
                    insert_val["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_val["race_type"] ="white"
                    insert_val["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_white"]
                    insert_val["participated"] = dat_frame.iloc[i]["white_participated"]
                    insert_val["participated_rate"] = dat_frame.iloc[i]["white_participated_rate"]
                    insert_val["unemployed"] = dat_frame.iloc[i]["white_unemployed"]
                    insert_val["unemployed_rate"] = dat_frame.iloc[i]["white_unemployed_rate"]
                    insert_list.append(insert_val)

                    insert_val1 = {}
                    insert_val1["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val1["race_type"] ="african american"
                    insert_val1["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_black"]
                    insert_val1["participated"] = dat_frame.iloc[i]["black_participated"]
                    insert_val1["participated_rate"] = dat_frame.iloc[i]["black_participated_rate"]
                    insert_val1["unemployed"] = dat_frame.iloc[i]["black_unemployed"]
                    insert_val1["unemployed_rate"] = dat_frame.iloc[i]["black_unemployed_rate"]
                    insert_val1["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val1)

                    insert_val2 = {}
                    insert_val2["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val2["race_type"] ="hispanic"
                    insert_val2["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_hlo"]
                    insert_val2["participated"] = dat_frame.iloc[i]["hlo_participated"]
                    insert_val2["participated_rate"] = dat_frame.iloc[i]["hlo_participated_rate"]
                    insert_val2["unemployed"] = dat_frame.iloc[i]["hlo_unemployed"]
                    insert_val2["unemployed_rate"] = dat_frame.iloc[i]["hlo_unemployed_rate"]
                    insert_val2["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val2)

                    insert_val3 = {}
                    insert_val3["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val3["race_type"] ="asian"
                    insert_val3["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_asian"]
                    insert_val3["participated"] = dat_frame.iloc[i]["asian_participated"]
                    insert_val3["participated_rate"] = dat_frame.iloc[i]["asian_participated_rate"]
                    insert_val3["unemployed"] = dat_frame.iloc[i]["asian_unemployed"]
                    insert_val3["unemployed_rate"] = dat_frame.iloc[i]["asian_unemployed_rate"]
                    insert_val3["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val3)
            except Exception as ex:
                print(ex)
                continue
            data_fr = pd.DataFrame(insert_list)
            data_fr.to_sql('unemployment_by_race_normalized_fact', con = engine, if_exists = 'append',chunksize = 1000, index= id)


    def create_race_fact(self):
        df = pd.read_sql("""
            SELECT date_id, civilian_noninstitutional_white, white_participated, white_participated_rate, white_unemployed, white_unemployed_rate, 
            civilian_noninstitutional_black, black_participated, black_participated_rate, black_unemployed, black_unemployed_rate, civilian_noninstitutional_hlo,
            hlo_participated, hlo_participated_rate, hlo_unemployed, hlo_unemployed_rate, civilian_noninstitutional_asian, asian_participated, asian_participated_rate, 
            asian_unemployed, asian_unemployed_rate,submission_date
            FROM unemployment_by_race_fact
            ORDER BY id
            """, con=engine,index_col=None,chunksize = 1000)


        column_names = ["date_id", "race_type", "civilian_noninstitutional","participated","unemployed","unemployed_rate"]
        data = pd.DataFrame(columns = column_names)

        for dat_frame in df:
            insert_list = []
            for i in range(dat_frame.shape[0]):
                insert_val = {}
                insert_val["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val["race_type"] ="white"
                insert_val["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_white"]
                insert_val["participated"] = dat_frame.iloc[i]["white_participated"]
                insert_val["participated_rate"] = dat_frame.iloc[i]["white_participated_rate"]
                insert_val["unemployed"] = dat_frame.iloc[i]["white_unemployed"]
                insert_val["unemployed_rate"] = dat_frame.iloc[i]["white_unemployed_rate"]
                insert_val["submission_date"] = dat_frame.iloc[i]["submission_date"]
                insert_list.append(insert_val)

                insert_val1 = {}
                insert_val1["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val1["race_type"] ="african american"
                insert_val1["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_black"]
                insert_val1["participated"] = dat_frame.iloc[i]["black_participated"]
                insert_val1["participated_rate"] = dat_frame.iloc[i]["black_participated_rate"]
                insert_val1["unemployed"] = dat_frame.iloc[i]["black_unemployed"]
                insert_val1["unemployed_rate"] = dat_frame.iloc[i]["black_unemployed_rate"]
                insert_val1["submission_date"] = dat_frame.iloc[i]["submission_date"]
                insert_list.append(insert_val1)

                insert_val2 = {}
                insert_val2["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val2["race_type"] ="hispanic"
                insert_val2["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_hlo"]
                insert_val2["participated"] = dat_frame.iloc[i]["hlo_participated"]
                insert_val2["participated_rate"] = dat_frame.iloc[i]["hlo_participated_rate"]
                insert_val2["unemployed"] = dat_frame.iloc[i]["hlo_unemployed"]
                insert_val2["unemployed_rate"] = dat_frame.iloc[i]["hlo_unemployed_rate"]
                insert_val2["submission_date"] = dat_frame.iloc[i]["submission_date"]
                insert_list.append(insert_val2)

                insert_val3 = {}
                insert_val3["date_id"] = dat_frame.iloc[i]["date_id"]
                insert_val3["race_type"] ="asian"
                insert_val3["civilian_noninstitutional"] =dat_frame.iloc[i]["civilian_noninstitutional_asian"]
                insert_val3["participated"] = dat_frame.iloc[i]["asian_participated"]
                insert_val3["participated_rate"] = dat_frame.iloc[i]["asian_participated_rate"]
                insert_val3["unemployed"] = dat_frame.iloc[i]["asian_unemployed"]
                insert_val3["unemployed_rate"] = dat_frame.iloc[i]["asian_unemployed_rate"]
                insert_val3["submission_date"] = dat_frame.iloc[i]["submission_date"]
                insert_list.append(insert_val3)
            data_fr = pd.DataFrame(insert_list)
            data_fr.drop_duplicates(subset=['date_id','race_type'], keep='first', inplace=True, ignore_index=False)
            data_fr.to_sql('unemployment_by_race_normalized_fact', con = engine, if_exists = 'append',chunksize = 1000, index= id)

    def create_education_fact(self):
        df = pd.read_sql("""
            SELECT date_id, less_than_high_school_participated, less_than_high_school_participated_rate, less_than_high_school_unemployed,
            less_than_high_school_unemployed_rate, high_school_grad_participated, high_school_grad_participated_rate, high_school_grad_unemployed,
            high_school_grad_unemployed_rate, some_college_associate_participated, some_college_associate_participated_rate, some_college_associate_unemployed,
            some_college_associate_unemployed_rate, bachelors_or_higher_college_associate_participated, bachelors_or_higher_college_associate_participated_rate, 
            bachelors_or_higher_college_associate_unemployed, bachelors_or_higher_college_associate_unemployed_rate,submission_date
            FROM unemployment_by_education_level_fact
            ORDER BY id
            """, con=engine,index_col=None,chunksize = 1000)

        column_names = ["date_id", "race_type", "civilian_noninstitutional","participated","unemployed","unemployed_rate"]
        data = pd.DataFrame(columns = column_names)

        for dat_frame in df:
            try:
                insert_list = []
                for i in range(dat_frame.shape[0]):
                    insert_val = {}
                    insert_val["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val["education"] ="less than high school"
                    insert_val["participated"] =dat_frame.iloc[i]["less_than_high_school_participated"]
                    insert_val["participated_rate"] = dat_frame.iloc[i]["less_than_high_school_participated_rate"]
                    insert_val["unemployed"] = dat_frame.iloc[i]["less_than_high_school_unemployed"]
                    insert_val["unemployed_rate"] = dat_frame.iloc[i]["less_than_high_school_unemployed_rate"]
                    insert_val["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val)

                    insert_val1 = {}
                    insert_val1["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val1["education"] ="high school grad"
                    insert_val1["participated"] =dat_frame.iloc[i]["high_school_grad_participated"]
                    insert_val1["participated_rate"] = dat_frame.iloc[i]["high_school_grad_participated_rate"]
                    insert_val1["unemployed"] = dat_frame.iloc[i]["high_school_grad_unemployed"]
                    insert_val1["unemployed_rate"] = dat_frame.iloc[i]["high_school_grad_unemployed_rate"]
                    insert_val1["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val1)

                    insert_val2 = {}
                    insert_val2["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val2["education"] ="some college associate"
                    insert_val2["participated"] =dat_frame.iloc[i]["some_college_associate_participated"]
                    insert_val2["participated_rate"] = dat_frame.iloc[i]["some_college_associate_participated_rate"]
                    insert_val2["unemployed"] = dat_frame.iloc[i]["some_college_associate_unemployed"]
                    insert_val2["unemployed_rate"] = dat_frame.iloc[i]["some_college_associate_unemployed_rate"]
                    insert_val2["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val2)

                    insert_val3 = {}
                    insert_val3["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val3["education"] ="bachelors or higher college associate"
                    insert_val3["participated"] =dat_frame.iloc[i]["bachelors_or_higher_college_associate_participated"]
                    insert_val3["participated_rate"] = dat_frame.iloc[i]["bachelors_or_higher_college_associate_participated_rate"]
                    insert_val3["unemployed"] = dat_frame.iloc[i]["bachelors_or_higher_college_associate_unemployed"]
                    insert_val3["unemployed_rate"] = dat_frame.iloc[i]["bachelors_or_higher_college_associate_unemployed_rate"]
                    insert_val3["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val3)
                data_fr = pd.DataFrame(insert_list)
                data_fr.drop_duplicates(subset=['date_id','education'], keep='first', inplace=True, ignore_index=False)
                data_fr.to_sql('unemployment_by_education_level_normalized_fact', con = engine, if_exists = 'append',chunksize = 1000, index= id)
            except Exception as e:
                print(e)
                continue
    def create_industry_fact(self):
        df = pd.read_sql("""
            SELECT date_id, total_unemployment, unemployment_rate, construction, construction_rate, manufacturing, manufacturing_rate, whole_sale_trade,
            whole_sale_trade_rate, transportation_and_utilities, transportation_and_utilities_rate, professional_business, professional_business_rate, 
            education_health_care, education_health_care_rate, leisure_hospitality, leisure_hospitality_rate, agriculture_related, agriculture_related_rate,submission_date
            FROM unemployment_by_industry_fact
            ORDER BY id
            """, con=engine,index_col=None,chunksize = 1000)

        for dat_frame in df:
            insert_list = []
            try:
                for i in range(dat_frame.shape[0]):
                    insert_val = {}
                    insert_val["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val["industry_type"] ="total"
                    insert_val["unemployment"] =dat_frame.iloc[i][1]
                    insert_val["unemployment_rate"] = dat_frame.iloc[i][2]
                    insert_val["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val)

                    insert_val1 = {}
                    insert_val1["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val1["industry_type"] ="construction"
                    insert_val1["unemployment"] =dat_frame.iloc[i][3]
                    insert_val1["unemployment_rate"] = dat_frame.iloc[i][4]
                    insert_val1["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val1)

                    insert_val2 = {}
                    insert_val2["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val2["industry_type"] ="manufacturing"
                    insert_val2["unemployment"] =dat_frame.iloc[i][5]
                    insert_val2["unemployment_rate"] = dat_frame.iloc[i][6]
                    insert_val2["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val2)

                    insert_val3 = {}
                    insert_val3["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val3["industry_type"] ="whole sale trade"
                    insert_val3["unemployment"] =dat_frame.iloc[i][7]
                    insert_val3["unemployment_rate"] = dat_frame.iloc[i][8]
                    insert_val3["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val3)

                    insert_val4 = {}
                    insert_val4["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val4["industry_type"] ="transportation and utilities"
                    insert_val4["unemployment"] =dat_frame.iloc[i][9]
                    insert_val4["unemployment_rate"] = dat_frame.iloc[i][10]
                    insert_val4["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val4)

                    insert_val5 = {}
                    insert_val5["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val5["industry_type"] ="professional business"
                    insert_val5["unemployment"] =dat_frame.iloc[i][11]
                    insert_val5["unemployment_rate"] = dat_frame.iloc[i][12]
                    insert_val5["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val5)

                    insert_val6 = {}
                    insert_val6["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val6["industry_type"] ="education health care"
                    insert_val6["unemployment"] =dat_frame.iloc[i][13]
                    insert_val6["unemployment_rate"] = dat_frame.iloc[i][14]
                    insert_val6["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val6)
                    insert_val7 = {}
                    insert_val7["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val7["industry_type"] ="leisure hospitality"
                    insert_val7["unemployment"] =dat_frame.iloc[i][15]
                    insert_val7["unemployment_rate"] = dat_frame.iloc[i][16]
                    insert_val7["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val7)
                    insert_val8 = {}
                    insert_val8["date_id"] = dat_frame.iloc[i]["date_id"]
                    insert_val8["industry_type"] ="agriculture related"
                    insert_val8["unemployment"] =dat_frame.iloc[i][17]
                    insert_val8["unemployment_rate"] = dat_frame.iloc[i][18]
                    insert_val8["submission_date"] = dat_frame.iloc[i]["submission_date"]
                    insert_list.append(insert_val8)

                data_fr = pd.DataFrame(insert_list)
                data_fr.drop_duplicates(subset=['date_id','industry_type','submission_date'], keep='first', inplace=True, ignore_index=False)
                data_fr.to_sql('unemployment_by_industry_normalized_fact', con = engine, if_exists = 'append',chunksize = 1000, index= id)
            except Exception as ex:
                print(ex)
                continue

    def load_state_unemployment_rate(self):
        date_df = pd.read_sql("SELECT * FROM housing_date_dim",con=engine)
        series_df = pd.read_sql("SELECT * FROM unemployment_series__dim",con=engine)
        state_df = pd.read_sql("SELECT * FROM state_dim",con=engine)
        date_df.reset_index(drop=True, inplace=True)
        for i in range(0,series_df.shape[0],25):

            with open("../datasets/unemployment_state" +str(i) +".json") as f:
                json_data = json.load(f)
                print(f.name)
            for series in json_data['Results']['series']:
                rate_list = []
                series_id =series['seriesID']
                try:
                    state_name = series_df.loc[series_df['area_code'] == series_id[3:-2]].iloc[0]['area_text']
                except Exception as ex:
                    print(series_id)
                    print(ex)
                state_id = state_df.loc[state_df['name']== state_name].iloc[0]['id']
                for item in series['data']:
                    row = {}
                    year = int(item["year"])
                    period = item['period']
                    value = item['value']
                    split = period.split("M")
                    month = int(split[1])
                    try:
                        date_id = date_df.loc[(date_df['year']==year) & (date_df['month']==month)].iloc[0]['id']
                    except Exception as ex:
                        print(ex)
                        continue
                    date = datetime.datetime(year,month,1)

                    row['date_id'] = date_id
                    row['state_id'] = state_id
                    row['unemployment_rate'] = value
                    row["submission_date"] = date
                    if value == "-":
                        continue
                    rate_list.append(row)
                try:
                    data_fr = pd.DataFrame(rate_list)
                    data_fr.drop_duplicates(subset=['date_id','state_id','submission_date'], keep='first', inplace=True, ignore_index=False)
                    data_fr.to_sql('unemployment_rate_by_state_fact', con = engine, if_exists = 'append',chunksize = 1000, index= id)
                except Exception as ex:
                    print(ex)
                    continue



if __name__ == "__main__":
    unemployment = Unemployment()
    unemployment.load_data()
    unemployment.create_education_fact()
    unemployment.create_industry_fact()
    unemployment.create_race_fact()
    unemployment.load_state_unemployment_rate()