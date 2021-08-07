from zipfile import ZipFile
from pymongo import MongoClient
import pandas as pd
from time import time
import wget
from time import time



wget.download('http://33f0f5018a2d.ngrok.io//get1')

test_file_name = 'file2.zip'

with ZipFile(test_file_name, 'r') as zip:
    
    zip.printdir()
    zip.extractall() 

## Importing all the data
client = MongoClient("mongodb://localhost:27017/")
db = client["partial2"]


#importing
start_time = time()

def filteringData():
    collect = db["states2"]
    df = pd.read_csv('subfile_2.csv',header = None)
    df.columns = ["FECHA_ACTUALIZACION","ID_REGISTRO","ORIGEN","SECTOR","ENTIDAD_UM","SEXO","ENTIDAD_NAC","ENTIDAD_RES","MUNICIPIO_RES","TIPO_PACIENTE","FECHA_INGRESO","FECHA_SINTOMAS","FECHA_DEF","INTUBADO","NEUMONIA","EDAD","NACIONALIDAD","EMBARAZO","HABLA_LENGUA_INDIG","INDIGENA","DIABETES","EPOC","ASMA","INMUSUPR","HIPERTENSION","OTRA_COM","CARDIOVASCULAR","OBESIDAD","RENAL_CRONICA","TABAQUISMO","OTRO_CASO","TOMA_MUESTRA_LAB","RESULTADO_LAB","TOMA_MUESTRA_ANTIGENO","RESULTADO_ANTIGENO","CLASIFICACION_FINAL","MIGRANTE","PAIS_NACIONALIDAD","PAIS_ORIGEN","UCI"]
    
    for state in range(1,33):
      deaths_state = len(df[(df["ENTIDAD_RES"] == state) & (df["FECHA_DEF"] != "9999-99-99")])
      active_state = len(df[(df["ENTIDAD_RES"] == state) & (df["FECHA_DEF"] == "9999-99-99")])
      collect.insert_one({"state":state, "deaths":deaths_state, "active":active_state})
      
filteringData()

elapsed_time = time() - start_time
print("\nElapsed time: %.10f seconds." % elapsed_time)
