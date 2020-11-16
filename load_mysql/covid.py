from sqlalchemy import text
import numpy as np
import datetime
from extract.covid_data import CovidData
import pandas as pd
from dbconfig import engine


class covid:
    select_state = """SELECT id FROM state_dim WHERE code =%s """
    select_country_name = """SELECT id FROM country_dim WHERE country_name =%s"""
    insert_country_query = text("""INSERT INTO country_dim(country_name) VALUES(:country_name) """)
    date_dim_query = text("""INSERT INTO covid_date_dim(day, month, year) VALUES(:day,:month,:year) """)
    select_usa_fact = """SELECT id from covid_usa_fact where date_id =%s AND state_id=%s"""
    select_world_fact = """SELECT id from covid_world_fact where date_id =%s AND country_id=%s"""
    select_date_dim_query = """SELECT id from covid_date_dim where day=%s and month=%s and year=%s """
    insert_usa_fact = text("""INSERT into covid_usa_fact( date_id, state_id, submit_date, new_deaths, new_cases, total_cases, total_deaths) 
    VALUES( :date_id, :state_id, :submit_date, :new_deaths, :new_cases, :total_cases, :total_deaths)""")
    insert_world_fact = text("""INSERT INTO covid_world_fact( date_id, country_id, continent, submission_date, new_deaths, new_cases, total_cases, total_deaths, total_cases_per_million, 
    total_deaths_per_million, new_cases_per_million, new_deaths_per_million, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, 
    weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, total_tests, new_tests, total_tests_per_thousand, new_tests_per_thousand, tests_per_case,
     positive_rate,  stringency_index, population, population_density, median_age, aged_65_older, aged_70_older, gdp_per_capita, extreme_poverty, cardiovasc_death_rate, diabetes_prevalence, handwashing_facilities, hospital_beds_per_thousand, life_expectancy, human_development_index)
    VALUES( :date_id, :country_id, :continent, :submission_date, :new_deaths, :new_cases, :total_cases, :total_deaths, :total_cases_per_million, :total_deaths_per_million, :new_cases_per_million, :new_deaths_per_million, :icu_patients, :icu_patients_per_million, :hosp_patients, :hosp_patients_per_million, :weekly_icu_admissions, :weekly_icu_admissions_per_million, :weekly_hosp_admissions, :weekly_hosp_admissions_per_million, :total_tests, :new_tests, :total_tests_per_thousand, :new_tests_per_thousand, :tests_per_case, :positive_rate,  :stringency_index, :population, :population_density, :median_age, :aged_65_older, :aged_70_older, :gdp_per_capita, :extreme_poverty, :cardiovasc_death_rate, :diabetes_prevalence, :handwashing_facilities, :hospital_beds_per_thousand, :life_expectancy, :human_development_index)""")
    covid_monthly_avg_country='''SELECT  c.country_name AS country,d.month AS month,d.year AS year,avg(cd.new_cases_per_million) AS avg_new_cases,
         avg(cd.new_deaths_per_million) as avg_new_deaths,cd.total_cases_per_million as total_cases_per_million, cd.total_deaths_per_million as total_deaths_per_million,cd.new_tests_per_thousand, 
         rank()  over(partition BY d.month,d.year ORDER BY AVG(cd.new_cases_per_million)  ) AS ranking 
         FROM covid_world_normalized_fact cd
         INNER JOIN covid_economy_impact.country_dim c ON cd.country_id = c.id
         INNER JOIN covid_economy_impact.covid_date_dim d on d.id = cd.date_id GROUP BY country,month'''
    def load_data(self):
        for key in CovidData.covid_config:
            self.fill_tables("../datasets/" + key + ".csv", key)
            '''self.fill_dimensions(Housing.sales_dim_csv)'''

    def fill_tables(self, file, key):
        df = pd.read_csv(file)
        columns = df.columns
        print(columns)
        print(df.shape)
        df = df.replace(np.nan, 0, regex=True)
        if key == "covid_us_data":
            df.drop_duplicates(keep='first', inplace=True, ignore_index=False)
        else:
            df.drop_duplicates( keep='first', inplace=True, ignore_index=False)
        print(df.shape)

        for i in range(df.shape[0]):
            with engine.connect() as conn:

                if key == "covid_us_data":
                    sub_date = df.iloc[i]["submission_date"]
                    state = df.iloc[i]["state"]
                    tot_cases = df.iloc[i]["tot_cases"]
                    new_cases = df.iloc[i]["new_case"]
                    tot_death = df.iloc[i]["tot_death"]
                    new_death = df.iloc[i]["new_death"]
                    date_time_obj = datetime.datetime.strptime(sub_date, '%m/%d/%Y')
                    date_args = {"day": date_time_obj.day, "month": date_time_obj.month, "year": date_time_obj.year}
                    date_id = self.execute_query(conn, self.select_date_dim_query, (date_time_obj.day, date_time_obj.month, date_time_obj.year))
                    if date_id is None:
                        result = conn.execute(self.date_dim_query, (date_args))
                        date_id = result.lastrowid
                    state_id = self.execute_query(conn, covid.select_state, (state))


                    result = conn.execute(self.select_usa_fact, (date_id, state_id))
                    if result.rowcount > 0:
                        continue
                    args = {"date_id": date_id, "state_id": state_id, "submit_date": date_time_obj, "new_deaths": new_death, "new_cases": new_cases,
                            "total_deaths": tot_death, "total_cases": tot_cases}
                    conn.execute(self.insert_usa_fact, args)

                else:
                    sub_date = df.iloc[i]["date"]
                    country = df.iloc[i]["location"]
                    total_cases = df.iloc[i]["total_cases"]
                    new_cases = df.iloc[i]["new_cases"]
                    total_deaths = df.iloc[i]["total_deaths"]
                    new_deaths = df.iloc[i]["new_deaths"]
                    total_cases_per_million = df.iloc[i]["total_cases_per_million"]

                    continent = df.iloc[i]["continent"]
                    total_deaths_per_million = df.iloc[i]["total_deaths_per_million"]
                    new_cases_per_million = df.iloc[i]["new_cases_per_million"]
                    new_deaths_per_million = df.iloc[i]["new_deaths_per_million"]

                    icu_patients = df.iloc[i]["icu_patients"]
                    icu_patients_per_million = df.iloc[i]["icu_patients_per_million"]
                    hosp_patients = df.iloc[i]["hosp_patients"]
                    hosp_patients_per_million = df.iloc[i]["hosp_patients_per_million"]
                    weekly_icu_admissions = df.iloc[i]["weekly_icu_admissions"]
                    weekly_icu_admissions_per_million = df.iloc[i]["weekly_icu_admissions_per_million"]
                    weekly_hosp_admissions = df.iloc[i]["weekly_hosp_admissions"]
                    weekly_hosp_admissions_per_million = df.iloc[i]["weekly_hosp_admissions_per_million"]

                    total_tests = df.iloc[i]["total_tests"]
                    new_tests = df.iloc[i]["new_tests"]
                    total_tests_per_thousand = df.iloc[i]["total_tests_per_thousand"]
                    new_tests_per_thousand = df.iloc[i]["new_tests_per_thousand"]
                    tests_per_case = df.iloc[i]["tests_per_case"]
                    positive_rate = df.iloc[i]["positive_rate"]
                    tests_units = df.iloc[i]["tests_units"]
                    stringency_index = df.iloc[i]["stringency_index"]

                    population = df.iloc[i]["population"]
                    population_density = df.iloc[i]["population_density"]
                    median_age = df.iloc[i]["median_age"]
                    aged_65_older = df.iloc[i]["aged_65_older"]
                    aged_70_older = df.iloc[i]["aged_70_older"]

                    gdp_per_capita = df.iloc[i]["gdp_per_capita"]
                    extreme_poverty = df.iloc[i]["extreme_poverty"]
                    cardiovasc_death_rate = df.iloc[i]["cardiovasc_death_rate"]
                    diabetes_prevalence = df.iloc[i]["diabetes_prevalence"]
                    handwashing_facilities = df.iloc[i]["handwashing_facilities"]
                    hospital_beds_per_thousand = df.iloc[i]["hospital_beds_per_thousand"]
                    life_expectancy = df.iloc[i]["life_expectancy"]
                    human_development_index = df.iloc[i]["human_development_index"]

                    date_time_obj = datetime.datetime.strptime(sub_date, '%Y-%m-%d')
                    date_args = {"day": date_time_obj.day, "month": date_time_obj.month, "year": date_time_obj.year}
                    date_id = self.execute_query(conn, self.select_date_dim_query, (date_time_obj.day, date_time_obj.month, date_time_obj.year))
                    if date_id is None:
                        result = conn.execute(self.date_dim_query, (date_args))
                        date_id = result.lastrowid
                    try:
                        country_args = {"country_name": country}
                        country_id = self.execute_query(conn, covid.select_country_name, country)

                    except Exception as ex:
                        print("Exception " + ex)
                        continue
                    if country_id is None:
                        result = conn.execute(covid.insert_country_query, country_args)
                        country_id = result.lastrowid

                    args = {"date_id": date_id, "country_id": country_id}
                    result = conn.execute(self.select_world_fact, (date_id, country_id))
                    if result.rowcount > 0:
                        continue
                    args = {"date_id": date_id, "country_id": country_id, "continent": continent, "submission_date": date_time_obj, "new_deaths": new_deaths, "new_cases": new_cases,
                            "total_cases": total_cases, "total_deaths": total_deaths, "total_cases_per_million": total_cases_per_million,
                            "total_deaths_per_million": total_deaths_per_million, "new_cases_per_million": new_cases_per_million,
                            "new_deaths_per_million": new_deaths_per_million, "icu_patients": icu_patients, "icu_patients_per_million": icu_patients_per_million,
                            "hosp_patients": hosp_patients, "hosp_patients_per_million": hosp_patients_per_million, "weekly_icu_admissions": weekly_icu_admissions,
                            "weekly_icu_admissions_per_million": weekly_icu_admissions_per_million, "weekly_hosp_admissions": weekly_hosp_admissions,
                            "weekly_hosp_admissions_per_million": weekly_hosp_admissions_per_million, "total_tests": total_tests, "new_tests": new_tests,
                            "total_tests_per_thousand": total_tests_per_thousand, "new_tests_per_thousand": new_tests_per_thousand, "tests_per_case": tests_per_case,
                            "positive_rate": positive_rate, "stringency_index": stringency_index, "population": population, "population_density": population_density,
                            "median_age": median_age, "aged_65_older": aged_65_older, "aged_70_older": aged_70_older, "gdp_per_capita": gdp_per_capita, "extreme_poverty": extreme_poverty,
                            "cardiovasc_death_rate": cardiovasc_death_rate, "diabetes_prevalence": diabetes_prevalence, "handwashing_facilities": handwashing_facilities,
                            "hospital_beds_per_thousand": hospital_beds_per_thousand, "life_expectancy": life_expectancy, "human_development_index": human_development_index}
                    conn.execute(self.insert_world_fact, args)

    def execute_query(self, conn, query, param):
        result = conn.execute(query, param)
        if result.rowcount > 0:
            row = result.fetchone()
            return row[0]
        else:
            print(param)

    def create_normalized_facts(self):
        df = pd.read_sql("""
            SELECT date_id, country_id,  submission_date, new_deaths, new_cases, total_cases, total_deaths, total_cases_per_million, 
                total_deaths_per_million, new_cases_per_million, new_deaths_per_million, icu_patients, icu_patients_per_million, hosp_patients, hosp_patients_per_million, weekly_icu_admissions, 
                weekly_icu_admissions_per_million, weekly_hosp_admissions, weekly_hosp_admissions_per_million, total_tests, new_tests, total_tests_per_thousand, new_tests_per_thousand, tests_per_case,
                 positive_rate,  stringency_index
            FROM covid_world_fact
            ORDER BY id
            """, con=engine, index_col=None, chunksize=1000)

        for dat_frame in df:
            print(dat_frame.shape)
            dat_frame.drop_duplicates(subset=['date_id','country_id'], keep='first', inplace=True, ignore_index=False)
            dat_frame.to_sql('covid_world_normalized_fact', con=engine, if_exists='append', chunksize=1000, index=id)

    def create_country_dim(self):
        df = pd.read_sql("""
            SELECT DISTINCT country_id, continent,   population, population_density, median_age, 
            aged_65_older, aged_70_older, gdp_per_capita, extreme_poverty, cardiovasc_death_rate,
            diabetes_prevalence, handwashing_facilities, hospital_beds_per_thousand, life_expectancy, 
            human_development_index
            FROM covid_world_fact
            ORDER BY country_id
            """, con=engine, index_col=None, chunksize=1000)

        for dat_frame in df:
            print(dat_frame.shape)
            try:
                dat_frame.to_sql('country_details_dim', con=engine, if_exists='append', chunksize=1000, index=False)
            except Exception as ex:
                print(ex)

    def covid_monthly_table(self):
        df = pd.read_sql(self.covid_monthly_avg_country, con=engine, index_col= None)
        df.to_sql('covid_monthly_avg_table', con = engine, if_exists = 'append', index= id)


if __name__ == "__main__":
    covid = covid()

    covid.load_data()
    covid.create_country_dim()
    covid.create_normalized_facts()
    try:
        covid.covid_monthly_table()
    except Exception as ex:
        print(ex)
