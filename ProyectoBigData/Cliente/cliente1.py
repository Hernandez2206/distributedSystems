from zipfile import ZipFile
from pymongo import MongoClient
import pandas as pd
from time import time
import wget



"""wget.download('http://33f0f5018a2d.ngrok.io//get')

test_file_name = 'file1.zip'

with ZipFile(test_file_name, 'r') as zip:
    
    zip.printdir()
    zip.extractall() 
    """
    
## Importing all the data
client = MongoClient("mongodb://localhost:27017/")
db = client["partial2"]

#importing
start_time = time()

def filteringData():
    collect = db["states1"]
    df = pd.read_csv('subfile_1.csv')

    for state in range(1,33):
      deaths_state = len(df[(df["ENTIDAD_RES"] == state) & (df["FECHA_DEF"] != "9999-99-99")])
      active_state = len(df[(df["ENTIDAD_RES"] == state) & (df["FECHA_DEF"] == "9999-99-99")])
      print(deaths_state)
      """collect.insert_one({"state":state, "deaths":deaths_state, "active":active_state})"""
      
filteringData()


elapsed_time = time() - start_time
print("\nElapsed time: %.10f seconds." % elapsed_time)
