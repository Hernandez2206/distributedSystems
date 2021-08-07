import shlex 
import requests
from zipfile import ZipFile
import csv
import itertools
import os
from time import time


# This is the request to download the file from the given url
start_time = time()
url = 'http://datosabiertos.salud.gob.mx/gobmx/salud/datos_abiertos/datos_abiertos_covid19.zip'
r = requests.get(url, stream = True)

with open("covid.zip", "wb") as Pypdf:
    for chunk in r.iter_content(chunk_size = 1024):
        if chunk:
            Pypdf.write(chunk)

# In this part of the code we are unzipping the file given by url
test_file_name = 'covid.zip'

with ZipFile(test_file_name, 'r') as zip:
    zip.printdir()
    zip.extractall()
    name = zip.namelist()
    zip.close()
name = name[0]

# In this part of the code we are splitting the file by 3 parts 


def rows_count(file):
    with open(file, 'r') as f:
        return sum(1 for row in f)

def get_chucks(it, lines, chucks):
    chuck_size = lines // chucks
    for i in range(1, chucks):
        yield i, itertools.islice(it, chuck_size)
    yield i + 1, it

def split_csv(file,  filediv,  header = True):
    with open(file, 'r') as csvfile:
        spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
        lines = rows_count(file)
        head = None
        if header:
            lines -= 1
            head = next(spamreader)
        if lines < filediv:
            raise ValueError("The number of rows ({}) is less than the number of output files ({})".format(lines, filediv))
        for i, data in get_chucks(spamreader, lines, filediv):
            path = "{}/subfile_{}.csv".format(os.path.dirname(file), i)
            write_csv_files(path, data, head)

def write_csv_files(path, data, header = None):
    with open(path, mode='w') as csvfile:
        spamwriter = csv.writer(csvfile, delimiter=',', quotechar='"',
                                quoting=csv.QUOTE_MINIMAL)
        if header:
            spamwriter.writerow(header)
        spamwriter.writerows(data)
        
# Splitting the file by 3 method
path = r'C:\Users\erick\Desktop\ProyectoBigData\Server'
path = path + "\\" + name 
print('The path is:',path)
split_csv(path,2,header = False)    
elapsed_time = time() - start_time
print("\nElapsed time: %.10f seconds." % elapsed_time)