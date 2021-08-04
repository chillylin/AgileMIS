import sqlite3
import pandas as pd


bfdatadate = "2020-12-31"
path = './'
dbfilename = 'test01'

con = sqlite3.connect(path+dbfilename+'.db')


# show funtion is used to get everything from a table/view
def show(table):
    return pd.read_sql_query("SELECT * FROM %s" %(table), con)

# Define a class to export dataframes into excels. 
class xp:
    container = {}
    
    def __init__(self):
        self.container = {}
    
    def i(self,content,name):
        self.container[name] = content
    
    def c(self, inputdict):
        self.container.update(inputdict)
    
    def w(self,filename):
        with pd.ExcelWriter(filename+'.xlsx') as writer:
            for key in self.container:
                self.container[key].to_excel(writer, sheet_name = key)

                
                
# get the data for a point: For financial position statement

## Firstly, get the accumulated data between the bring forward date to the cutting date
### There are two sources for accumulating data: Bank transaction (Query Gamma), non-banktransaction(Query Delta). 
#### For bank transactions, two query is needed to get the bank side (Query Beta) and the opposite side (Query Alpha). 
## Then add the bring forward 

def create_view_fp_as_at(viewname,cuttingdate):
    
    bf_date = bfdatadate
    
    # (Query Alfa0): 
    # Notice the amount is negative because the amount is recorded as transaction of bank
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_nonbank_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_banktransaction_nonbank_for_%s AS

    SELECT 
        ma_account_id,
        ma_account_name,
        -SUM(IIF(is_cancelling IS NULL,1,-1)*(amount-vat)) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 

        GROUP BY ma_account_id

    """%(viewname,bf_date, cuttingdate) )
    
    # (Query Alfa10): 
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_vat0_for_%s;'%(viewname)
    )
    
    con.execute("""
    CREATE VIEW accumulated_banktransaction_vat0_for_%s AS

    SELECT 
        'MA22210101' AS ma_account_id,
        'VAT_in' AS ma_account_name,
        -SUM(IIF(is_cancelling IS NULL,1,-1)*IIF(vat < 0,1,0)*(vat)) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 



    """%(viewname,bf_date, cuttingdate) )
    
    # (Query Alfa10): 
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_vat1_for_%s;'%(viewname)
    )
    
    con.execute("""
    CREATE VIEW accumulated_banktransaction_vat1_for_%s AS

    SELECT 
        'MA22210102' AS ma_account_id,
        'VAT_out' AS ma_account_name,
        -SUM(IIF(is_cancelling IS NULL,1,-1)*IIF(vat > 0,1,0)*(vat)) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 



    """%(viewname,bf_date, cuttingdate) )
    
    # (Query Beta)
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_bank_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_banktransaction_bank_for_%s AS

    SELECT 
        'MA1002' AS ma_account_id,
        'Bank' AS ma_account_name,
        SUM(IIF(is_cancelling IS NULL,1,-1)*amount) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 

/*        GROUP BY bank_account_id */

    """%(viewname,bf_date, cuttingdate) )
    
    # (Query Gamma)
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_banktransaction_for_%s AS

    SELECT * FROM accumulated_banktransaction_bank_for_%s
    UNION
    SELECT * FROM accumulated_banktransaction_vat0_for_%s
    UNION
    SELECT * FROM accumulated_banktransaction_vat1_for_%s
    UNION
    SELECT * FROM accumulated_banktransaction_nonbank_for_%s

    """%(viewname,viewname,viewname,viewname,viewname))
        
    
   #(Query Delta)
    con.execute(
    'DROP VIEW IF EXISTS accumulated_ADJ_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_ADJ_for_%s AS

    SELECT 
        ma_account_id, 
        ma_account_name, 
        IFNULL(SUM(debit),0)-IFNULL(SUM(credit),0) AS amount
    FROM AJE 

    WHERE 
        JULIANDAY(date)>JULIANDAY("%s") 
        AND  
        JULIANDAY(date)<=JULIANDAY("%s") 

    GROUP BY ma_account_id
     """%(viewname,bf_date, cuttingdate) )
    
    
    
    # adding bank transaction from Query Gamma and adjustment transaction from Query Delta into the final result
    con.execute(
    'DROP VIEW IF EXISTS balance_at_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW balance_at_%s AS

    SELECT 
        ma_account_id,
        ma_account_name,
        IFNULL(bf_amount,0) + IFNULL(bankamount,0)+IFNULL(adjamount,0) AS Amount

    FROM
        (
        SELECT * FROM chart_of_accounts
        LEFT JOIN (SELECT ma_account_id AS b_id, amount as bankamount FROM accumulated_banktransaction_for_%s) ON chart_of_accounts.ma_account_id = b_id
        LEFT JOIN (SELECT ma_account_id AS j_id, amount as adjamount FROM accumulated_ADJ_for_%s) ON chart_of_accounts.ma_account_id = j_id
        LEFT JOIN account_bf ON chart_of_accounts.ma_account_id = account_bf.ma_account_id)
    """%(viewname,viewname,viewname))
    
def show_fp_as_at(cuttingdate):    
    create_view_fp_as_at('frtemp',cuttingdate)
    return pd.read_sql_query("SELECT * FROM balance_at_frtemp", con)

# Get duration data for income statement

def create_view_pl_for(viewname, starting, ending):

    # (Query Alfa0: 
    # Notice the amount is negative because the amount is recorded as transaction of bank
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_nonbank_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_banktransaction_nonbank_for_%s AS

    SELECT 
        ma_account_id,
        ma_account_name,
        -SUM(IIF(is_cancelling IS NULL,1,-1)*(amount-vat)) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 

        GROUP BY ma_account_id

    """%(viewname,starting, ending) )
    
    
    # (Query Alfa10): 
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_vat0_for_%s;'%(viewname)
    )
    
    con.execute("""
    CREATE VIEW accumulated_banktransaction_vat0_for_%s AS

    SELECT 
        'MA22210101' AS ma_account_id,
        'VAT_in' AS ma_account_name,
        -SUM(IIF(is_cancelling IS NULL,1,-1)*IIF(vat < 0,1,0)*(vat)) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 



    """%(viewname,starting, ending) )
    
    # (Query Alfa10): 
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_vat1_for_%s;'%(viewname)
    )
    
    con.execute("""
    CREATE VIEW accumulated_banktransaction_vat1_for_%s AS

    SELECT 
        'MA22210102' AS ma_account_id,
        'VAT_out' AS ma_account_name,
        -SUM(IIF(is_cancelling IS NULL,1,-1)*IIF(vat > 0,1,0)*(vat)) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 



    """%(viewname,starting, ending) )
    
    
    
    
    # (Query Beta)
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_bank_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_banktransaction_bank_for_%s AS

    SELECT 
        'MA1002' AS ma_account_id,
        'Bank' AS ma_account_name,
        SUM(IIF(is_cancelling IS NULL,1,-1)*amount) AS Amount

        FROM bankstatement 

        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 


    """%(viewname,starting, ending) )
    
    # (Query Gamma)
    con.execute(
    'DROP VIEW IF EXISTS accumulated_banktransaction_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_banktransaction_for_%s AS

    SELECT * FROM accumulated_banktransaction_bank_for_%s
    UNION
    SELECT * FROM accumulated_banktransaction_vat0_for_%s
    UNION
    SELECT * FROM accumulated_banktransaction_vat1_for_%s
    UNION
    SELECT * FROM accumulated_banktransaction_nonbank_for_%s

    """%(viewname,viewname,viewname,viewname,viewname))
    
        
    
   #(Query Delta)
    con.execute(
    'DROP VIEW IF EXISTS accumulated_ADJ_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW accumulated_ADJ_for_%s AS

    SELECT 
        ma_account_id, 
        ma_account_name, 
        IFNULL(SUM(debit),0)-IFNULL(SUM(credit),0) AS amount
    FROM AJE 

    WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 

    GROUP BY ma_account_id
     """%(viewname,starting, ending) )
    
    
    
    # adding bank transaction from Query Gamma and adjustment transaction from Query Delta into the final result
    con.execute(
    'DROP VIEW IF EXISTS during%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW during%s AS

    SELECT 
        ma_account_id,
        ma_account_name,
        IFNULL(bankamount,0)+IFNULL(adjamount,0) AS Amount

    FROM
        (
        SELECT * FROM chart_of_accounts
        LEFT JOIN (SELECT ma_account_id AS b_id, amount as bankamount FROM accumulated_banktransaction_for_%s) ON chart_of_accounts.ma_account_id = b_id
        LEFT JOIN (SELECT ma_account_id AS j_id, amount as adjamount FROM accumulated_ADJ_for_%s) ON chart_of_accounts.ma_account_id = j_id
        )
    """%(viewname,viewname,viewname))
    
    return show('during'+viewname)

def show_pl_for(starting, ending):    
    create_view_pl_for("pltemp", starting, ending)
    return pd.read_sql_query("SELECT * FROM duringpltemp", con)

# create a view for the cashflow between two dates for cashflow reports
def create_view_cf_for(viewname, starting, ending):

    con.execute(
    'DROP VIEW IF EXISTS CF_for_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW CF_for_%s AS

    SELECT 
        Suggested_CF, 
        cf_account_id,

        -SUM(IIF(is_cancelling IS NULL,1,-1)*(amount-vat)) AS Amount

        FROM bankstatement 


        WHERE JULIANDAY(date)>JULIANDAY("%s") 
        AND  JULIANDAY(date)<=JULIANDAY("%s") 
        
    GROUP BY Suggested_CF


    """%(viewname,starting, ending) )
    
# Get the data between two dates for cashflow reports
def show_cf_for(starting, ending):    
    create_view_cf_for("cftemp", starting, ending)
    return pd.read_sql_query("SELECT * FROM CF_for_cftemp", con)

# Compile data into financial reports according to the template and mapping
# A template is the format of a report. From each item to the final report
# A mapping is the mapping from each account to each item
# Template and mapping files are provided in this repository as sample csv files,
# so that the reports can be changed via the csv files, without change the python code.
  
def statementpreparing(reporttemplate, mapping, inputdf, reporttype, title):
    
    if reporttype == 'CF':
        # Applying mapping and reporting template to data
        returndf = reporttemplate.join(inputdf.join(mapping[['cf_account_id',reporttype]].set_index('cf_account_id'),
                                on = 'Suggested_CF')[[reporttype,'Amount']].groupby(reporttype).sum(), on = '数据来源')
    else: 
        # Applying mapping and reporting template to data
        returndf = reporttemplate.join(inputdf.join(mapping[['ma_account_id',reporttype]].set_index('ma_account_id'),
                                on = 'ma_account_id')[[reporttype,'Amount']].groupby(reporttype).sum(), on = '数据来源')
    
    
    
    # Accumulating data by mapping rules
    for i in range(4):

        temp = returndf[['add'+str(i)+'i','Amount']].groupby('add'+str(i)+'i').sum()
        temp.columns = [str(i)+'o']

        returndf = returndf.join(temp, on = 'add'+str(i)+'o')
    
    # Calculating final return
    returndf[title] = (returndf[['Amount','0o','1o','2o','3o']].sum(axis = 1)*returndf['Direction'])
    
    return returndf[['Report',title]]#.fillna('').replace({0:''})
    
# Usage start here

# Test VAT error. 
# The VAT shall be a number instead of a blank. 
# a blank need to be detected since a blank means the preparer forgot to input VAT
# a zero VAT item shall be inputed as an error.
# Missing such input will be found here
pd.read_sql_query("SELECT * FROM bankstatement WHERE VAT IS NULL", con)

# Read mapping and format file
formatpath = './reportFormat/'
formatname = 'FRmapping.csv'
mappingname = 'Financial Reports.csv'

frmapping = pd.read_csv(formatpath+formatname)
fpmapping = frmapping
plmapping = frmapping[frmapping]
# The line number shall be adjusted according to the csv file

cfmapping = frmapping.iloc[235:,:]
# The line number shall be adjusted according to the csv file
cfmapping.columns = ['cf_account_id', 'cf_account_name', 'FP', 'PL', 'CF']
cfmapping['cf_account_id'] = cfmapping['cf_account_id'].astype(int)

frtemplate = pd.read_csv(formatpath+mappingname)
pltemplate = frtemplate[~frtemplate['FP'].isnull()]
fptemplate = frtemplate[~frtemplate['PL'].isnull()]
cftemplate = frtemplate[~frtemplate['CF'].isnull()]

# Setting up the date parameter
previousperiodend = "2020-12-31" # Must in text datatype
periods = 12
frequency = "M" # "M" for monthly reports. "Y" for annual reports.

cuttingdates = pd.date_range(previousperiodend, periods=periods+1, freq=frequency)

# Process reports for each dates
fp = []
pl = []
cf = []

for closing in cuttingdates.astype(str):
    
    fp.append(
        statementpreparing(
            fptemplate, 
            fpmapping, 
            show_fp_as_at(closing[5:7],closing), 
            'FP', 
            closing[5:7]).set_index('Report')    
    )
    
    try:
        pl.append(
            
            statementpreparing(
                pltemplate, 
                plmapping, 
                show_pl_for(closing[5:7],opening,closing), 
                'PL', 
                closing[5:7]).set_index('Report')
            )
        
        cf.append(
            
            statementpreparing(
                cftemplate, 
                cfmapping, 
                show_cf_for(closing[5:7],opening,closing), 
                'CF', 
                closing[5:7]).set_index('Report')
            )
    except:
        pass
    
    
    opening = closing

# Combine reports into one dataframe for each type of reports.
fps = pd.concat(fp, axis = 1)
pls = pd.concat(pl, axis = 1)
cfs = pd.concat(cf, axis = 1)

# Write to report file

finalreportname = 'testreport'

h = xp()
h.i(fps,'FPS')
h.i(pls,"PLS")
h.i(cfs,"CFS")
h.w(finalreportname)              
