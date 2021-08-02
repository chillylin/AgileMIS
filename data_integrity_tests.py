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

def reconcile_bf_account_vs_externalparties(conn):

    conn.execute(
    'DROP VIEW IF EXISTS ReconcileEPBF ;'
    )

    conn.execute("""
    CREATE VIEW ReconcileEPBF AS

    SELECT ma_account_id, bf_amount - EPSUM AS BFDIFF FROM (SELECT ma_account_id AS MID, SUM(amount) AS EPSUM FROM ep_bf GROUP BY ma_account_id)
    LEFT JOIN (SELECT bf_amount, ma_account_id FROM account_bf) on MID = ma_account_id
    """)
    return pd.read_sql_query("SELECT * FROM ReconcileEPBF" %(table), conn)

reconcile_bf_account_vs_externalparties(con)

def check_bankstatement_for_missing_ep_id(conn):
    conn.execute(
    'DROP VIEW IF EXISTS invalid_ep_id_bank ;'
    )

    conn.execute("""
    CREATE VIEW invalid_ep_id_bank AS

    SELECT * FROM (SELECT ma_account_id AS MID FROM chart_of_accounts WHERE AP_AR = 1)
    LEFT JOIN ( SELECT ma_account_id AS MID2, ep_id, amount FROM bankstatement) 
    ON MID = MID2 WHERE NOT MID2 IS NULL AND ep_id IS NULL
    """)
    
    return pd.read_sql_query("SELECT * FROM invalid_ep_id_bank", conn)
check_bankstatement_for_missing_ep_id(con)

def check_AJE_for_missing_ep_id(conn):

    conn.execute(
    'DROP VIEW IF EXISTS invalid_ep_id_AJE ;'
    )

    conn.execute("""
    CREATE VIEW invalid_ep_id_AJE AS

    SELECT * FROM (SELECT ma_account_id AS MID FROM chart_of_accounts WHERE AP_AR = 1)
    LEFT JOIN ( SELECT ma_account_id AS MID2, ep_id, amount FROM AJE) 
    ON MID = MID2 WHERE NOT MID2 IS NULL AND ep_id IS NULL
    """)
    return pd.read_sql_query("SELECT * FROM invalid_ep_id_AJE", conn)
check_AJE_for_missing_ep_id(con)
