import sqlite3
import pandas as pd

path = './'
dbfilename = 'test01'

con = sqlite3.connect(path+dbfilename+'.db')

def is_relocation_nagative(conn):
    
    df = pd.read_sql_query("SELECT * from relocation", conn)
    
    df['dquantity'] = df['quantity']*df['direction']

    for date in pd.unique(df['date']):
        slicedf = df[df['date']<= date].copy()

        if (int(slicedf.groupby(['q_id','site_name','equip_type_name']).sum()[['dquantity']].min())<0):
            print ('error'+str(date))
            return date
    return 0

is_relocation_nagative(con)

