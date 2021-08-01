import pandas as pd
import os
import sqlite3

path  = './'
dbfilename = 'testdb'
sourcefilename = 'schedules'

try:
    sourcefile = pd.read_excel(path+sourcefilename+'.xlsx', sheet_name=None)
except:
    print ('Cannot open sourcefile')

try:
    os.remove(path+dbfilename+'.db')
except:
    pass

con = sqlite3.connect(path+dbfilename+'.db')

for sheetname in sourcefile:
    sourcefile[sheetname].to_sql(sheetname, con )
con.close()
