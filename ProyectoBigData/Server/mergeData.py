from pymongo import MongoClient
import json
import pandas as pd


#Cargamos la base de datos con sus dos colleciones, una para cada nodo
client = MongoClient("mongodb://localhost:27017/")
db = client["partial2"]
col_1 = db["states1"]
col_2 = db["states2"]

#Funcion para el merge
def mergeFiles():
    
    #Encontramos todos los valores de cada coleccion y las guardamos
    data1 = list(col_1.find())
    data2 = list(col_2.find())

    info = data1
    #merge
    info.extend(data2)
    
    info_merged = pd.DataFrame(info).groupby(by=['state']).sum()
    

    #imprimir json
    complete = []
    for state in info_merged.iterrows():
        grl = state[1].to_dict()
        data = {"Estado":state[0], "Activos": grl["active"], "Muertes": grl["deaths"]}
        complete.append(data)
          
    file = open("data.json", "w")
    json.dump(complete, file)
    
mergeFiles()

#Ejecutamos el script llamado dataMerge.py
try:
    exec(open("dataMerge.py").read())
except:
    print("The dataMerge.py file failed")
else:
    print("It was sucessfully")    
    



    


