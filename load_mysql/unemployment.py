from extract.unemploymentdata import UnemploymentData
import json
from sqlalchemy import text
from dbconfig import engine

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
        "by_race":text("""INSERT INTO unemployment_by_race_fact(date_id, civilian_noninstitutional_white, white_participated, white_participated_rate, white_unemployed, white_unemployed_rate, civilian_noninstitutional_black, black_participated, black_participated_rate, black_unemployed, black_unemployed_rate, civilian_noninstitutional_hlo, hlo_participated, hlo_participated_rate, hlo_unemployed, hlo_unemployed_rate, civilian_noninstitutional_asian, asian_participated, asian_participated_rate, asian_unemployed, asian_unemployed_rate) 
        VALUES(:date_id, :civilian_noninstitutional_white, :white_participated, :white_participated_rate, :white_unemployed, :white_unemployed_rate, :civilian_noninstitutional_black, :black_participated, :black_participated_rate, :black_unemployed, :black_unemployed_rate, :civilian_noninstitutional_hlo, :hlo_participated, :hlo_participated_rate, :hlo_unemployed, :hlo_unemployed_rate, :civilian_noninstitutional_asian, :asian_participated, :asian_participated_rate, :asian_unemployed, :asian_unemployed_rate)"""),
        "by_education_level":text("""INSERT INTO unemployment_by_education_level_fact( date_id, less_than_high_school_participated, less_than_high_school_participated_rate, less_than_high_school_unemployed, less_than_high_school_unemployed_rate, high_school_grad_participated, high_school_grad_participated_rate, high_school_grad_unemployed, high_school_grad_unemployed_rate, some_college_associate_participated, some_college_associate_participated_rate, some_college_associate_unemployed, some_college_associate_unemployed_rate, bachelors_or_higher_college_associate_participated, bachelors_or_higher_college_associate_participated_rate, bachelors_or_higher_college_associate_unemployed, bachelors_or_higher_college_associate_unemployed_rate) 
        VALUES( :date_id, :less_than_high_school_participated, :less_than_high_school_participated_rate, :less_than_high_school_unemployed, :less_than_high_school_unemployed_rate, :high_school_grad_participated, :high_school_grad_participated_rate, :high_school_grad_unemployed, :high_school_grad_unemployed_rate, :some_college_associate_participated, :some_college_associate_participated_rate, :some_college_associate_unemployed, :some_college_associate_unemployed_rate, :bachelors_or_higher_college_associate_participated, :bachelors_or_higher_college_associate_participated_rate, :bachelors_or_higher_college_associate_unemployed, :bachelors_or_higher_college_associate_unemployed_rate) """) ,
        "by_industry": text("""INSERT into unemployment_by_industry_fact( date_id, total_unemployment, unemployment_rate, construction, construction_rate, manufacturing, manufacturing_rate, whole_sale_trade, whole_sale_trade_rate, transportation_and_utilities, transportation_and_utilities_rate, professional_business, professional_business_rate, education_health_care, education_health_care_rate, leisure_hospitality, leisure_hospitality_rate, agriculture_related, agriculture_related_rate)
         VALUES( :date_id, :total_unemployment, :unemployment_rate, :construction, :construction_rate, :manufacturing, :manufacturing_rate, :whole_sale_trade, :whole_sale_trade_rate, :transportation_and_utilities, :transportation_and_utilities_rate, :professional_business, :professional_business_rate, :education_health_care, :education_health_care_rate, :leisure_hospitality, :leisure_hospitality_rate, :agriculture_related, :agriculture_related_rate)""")
    }
    select_date_dim_query = """SELECT id from housing_date_dim where month=%s and year=%s """



    def load_data(self):
        for key in UnemploymentData.series:
            print()
            print()
            print(key)
            with open("../datasets/"+key+".json") as f:
                json_data = json.load(f)

            for i in range(len(json_data['Results']['series'][0]["data"])):
                row_data = dict()
                with engine.connect() as conn:
                    for j in range(len(json_data['Results']['series'])):
                        series_id =json_data['Results']['series'][j]['seriesID']
                        year = json_data['Results']['series'][j]["data"][i]["year"]
                        period = str(json_data['Results']['series'][j]["data"][i]['period'])
                        value = json_data['Results']['series'][j]["data"][i]['value']
                        split = period.split("M")
                        month = split[1]
                        row_data[self.series_column.get(series_id)] = value
                        result = conn.execute(self.select_date_dim_query,(month,year))
                        if result.rowcount>0:
                            row = result.fetchone()
                            date_id=row[0]
                        row_data["date_id"] = date_id

                    print("series_id"+series_id +""+value)
                    print(row_data)
                    print(self.query.get(key))
                    print( json.dumps(row_data))
                    conn.execute(self.query.get(key), row_data)





if __name__ == "__main__":
    unemployment = Unemployment()
    unemployment.load_data()