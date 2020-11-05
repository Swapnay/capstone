import pandas as pd
import os

entries = os.listdir("/Users/syeruvala/Downloads/data/")
for entry in entries:
    try:
        df =pd.read_csv("/Users/syeruvala/Downloads/data/"+entry)
        df.to_csv("../datasets/"+entry,index=False)
    except Exception as ex:
        print(ex)
        continue