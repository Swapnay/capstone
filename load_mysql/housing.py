from extract.housing_data import config
import pandas as pd
import csv

class Housing:

    def load(self):
        for key in config:
            df= pd.read_csv("../datasets/"+key+".csv")
            for row in df.iterrows():
                print(row)


housing = Housing()
housing.load()
