import json
import pandas as pd
from pandas import DataFrame



#Load the both json files
with open('data.json') as f:
    data = json.load(f)

with open('diccionario1.json') as g:
    data1 = json.load(g)
    
#Convierte los Json en un dataframe para una mejor manipulacion
df = pd.DataFrame(data)
df1 = pd.DataFrame(data1)


n = len(df1['Estado'])#Para saber cuantos estados hay
#Esto reemplaza el numero por el nombre del estado
for state in range(n):
    df['Estado'] = df['Estado'].replace(state+1,df1['Estado'][state])
 

complete = [] #Array para el guardado del Json

#Creacion del Json
for state1 in range(n):
    dataModified = {"nombre":df['Estado'][state1],"infectados":df['Activos'][state1],"defunciones":df['Muertes'][state1]}
    complete.append(dataModified)
    
#Creacion del dataframe
df2 = pd.DataFrame(complete)

#Abrimos el archivo del maestro para agregar los valores de Capital, lat y lon

    


#Arrays auxiliares
complete1 = []
muertes1 = []
activos1 = []


for state2 in range(n):
    #Cambiamos los valores int de "defunciones" y de "infectados" a strings para la lectura de la app
    muertes = str(df2['defunciones'][state2])
    activos = str(df2['infectados'][state2])
    #Guardamos en el array
    muertes1.append(muertes)
    activos1.append(activos)
    #Creacion del formato Json
    dataModified1 = {"nombre":df2['nombre'][state2],"Capital":df1['Capital'][state2],"lat":df1['lat'][state2],"lon":df1['lon'][state2],"infectados":activos1[state2],"defunciones":muertes1[state2]}
    complete1.append(dataModified1)
    
#Creacion del archivo Json
df4 = pd.DataFrame(complete1)
df4.to_json("covid.json",orient='records',lines=False)
    






