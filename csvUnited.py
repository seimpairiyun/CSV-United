#https://medium.com/@stella96joshua/how-to-combine-multiple-csv-files-using-python-for-your-analysis-a88017c6ff9e

import pandas as pd
import glob
import csv
import sys
import os

#-----------------------------------------------------

targetFile = sys.argv[1]

def readCSVinDirectory():
    return glob.glob(f'{targetFile}-*.CSV')

def delTempFile():
    os.system(f'del {targetFile}-*.csv')
    print('Finished')

def processINFO(msg):
    for n,file in enumerate(readCSVinDirectory()):
        #idx = str(n+1)
        print(f'{file} {msg}')

def processCSV(sep):
    #1. Read all csv and merger
    master = pd.concat(
                [pd.read_csv(
                    file,
                    dtype="str",
                    sep=sep,
                ) for file in readCSVinDirectory() ], 
                ignore_index=True
             )
        
    #Remove double quote in header
    #master.columns.str.replace('"', "")
    #master.columns = [col[1:-1] for col in master.columns]
    
    return master

def exportCSV(file, outputName):
    file.to_csv(
        f'{outputName}.csv', 
        sep=';', 
        index=False,
        quoting=csv.QUOTE_ALL
    )    
    
def processCSVwithUnknownDelimiter(file, sep):
    data = pd.read_csv(
            f'{file}', 
            dtype="str",
            quoting=csv.QUOTE_ALL,
            sep=sep,
        )
    
    #Drop All Unnamed Column
    data.drop(data.columns[data.columns.str.contains('Unnamed',case = False)],axis = 1, inplace = True)

    return data

def processCSV_Again():
    for n,file in enumerate(readCSVinDirectory()):
        data = processCSVwithUnknownDelimiter(file,'|')
        exportCSV(data, f'{file}-temp')
        os.system(f'del {file}')


# Run Merge CSV Tools
try:
    data = processCSV(';')
    exportCSV(data, targetFile)
    
    processINFO('joined.')
    
    delTempFile()
except:
    processCSV_Again()
    data = processCSV(';')
    exportCSV(data, targetFile)
    
    processINFO('cleansing done.')
    processINFO('joined.')
    
    delTempFile()
    
    
