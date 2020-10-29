import pandas as pd
pd.set_option('display.max_columns', 7)
covid_data = pd.read_csv("https://data.cdc.gov/api/views/9mfq-cb36/rows.csv?accessType=DOWNLOAD")

print(covid_data[["submission_date", "state", "tot_cases", "new_case", "tot_death","new_death"]][100:110])

covid_data.to_csv("../datasets/cdc_covid_usa.csv")

covid_world_data = pd.read_csv("https://covid19.who.int/WHO-COVID-19-global-data.csv")
#[["Date_reported", "Country", "New_cases","Cumulative_cases","New_deaths",  "Cumulative_deaths"]]
print(covid_world_data.columns)
print(covid_world_data[["Date_reported", " Country", " New_cases"," Cumulative_cases"," New_deaths",  " Cumulative_deaths"]][100:110])

covid_world_data.to_csv("../datasets/who_covid_world.csv")



