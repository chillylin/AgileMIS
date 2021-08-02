def create_view_paymentprocess(year, month):

    con.execute(
    'DROP VIEW IF EXISTS payroll%s%s;'%(year, month)
    )

    con.execute("""
    CREATE VIEW payroll%s%s AS

    SELECT * FROM ( SELECT  
        worker_id AS left_worker_id, 
        total_salary, individual_income_tax, social_security_employee, house_tax,
        social_security_employer,
        Social_security_recoverable
    FROM payroll 
        WHERE year = %s 
        AND month = %s )

    JOIN  

    (SELECT worker_id AS right_worker_id, department FROM worker_info)


    ON left_worker_id = right_worker_id
    """%(year, month, year, month))

def payrollregular(year, month):
    
    print (year)
    print (month)
    
    create_view_paymentprocess(year, month)
    
    # Processing part 1: regular employees
    con.execute(
    'DROP VIEW IF EXISTS payrollinternal%s%s;'%(year, month)
    )
    con.execute("""
    CREATE VIEW payrollinternal%s%s AS

    SELECT left_worker_id AS worker_id, 
        total_salary, individual_income_tax
    , social_security_employee, house_tax,
        social_security_employer,
        department
    FROM payroll%s%s

    WHERE Social_security_recoverable IS NULL
    """%(year, month, year, month))
    
    regtdf = show('payrollinternal%s%s'%(year, month)).groupby('department').sum()
    regtdf = pd.DataFrame(regtdf[regtdf.columns[1:]].sum())
    regtdf.columns = ['DR']

    returndf = pd.concat([regtdf,
               pd.DataFrame({'CR': [regtdf.loc['total_salary'][0],
                                 regtdf.loc['individual_income_tax'][0] + regtdf.loc['social_security_employee'][0] + regtdf.loc['house_tax'][0],
                                 regtdf.loc['social_security_employer'][0]
                                ]}, 
                            index = ['total_salary bal','Deductions','ss_empolyer'])
              ], axis = 1).fillna(0)
    returndf['ma_account_id'] = [
        'MA560207',
        'MA221101',
        'MA221101',
        'MA221101',
        'MA560216',
        'MA221101',
        'MA2242',
        'MA221104'
    ]

    returndf['date'] = year+'/'+month

    return returndf

def payrollassociate(year, month):

    con.execute(
    'DROP VIEW IF EXISTS payrollexternal%s%s;'%(year, month)
    )
    con.execute("""
    CREATE VIEW payrollexternal%s%s AS

    SELECT left_worker_id AS worker_id, 
        total_salary, individual_income_tax
    , social_security_employee, house_tax,
        social_security_employer,
        Social_security_recoverable,
        department
    FROM payroll%s%s

    WHERE NOT Social_security_recoverable = 1
    """%(year, month, year, month))
    
    sssum = show('payrollexternal%s%s'%(year, month))['social_security_employer'].sum()
    return pd.DataFrame({'DR':[sssum,0], 
                         'CR':[0,sssum],
                         'ma_account_id':['MA560234','MA221104'],
                         'date':[year+'/'+month,year+'/'+month]}, 
                        index = ['Social security paid for rp','Social security paid for rp'])

def payrollexternal(year, month):
    create_view_paymentprocess(year, month)
    
    con.execute(
    'DROP VIEW IF EXISTS socialsecurityagency%s%s;'%(year, month)
    )
    con.execute("""
    CREATE VIEW socialsecurityagency%s%s AS

    SELECT left_worker_id AS worker_id, 
        total_salary, individual_income_tax
    , social_security_employee, house_tax,
        social_security_employer,
        Social_security_recoverable,
        department
    FROM payroll%s%s

    WHERE Social_security_recoverable = 1
    """%(year, month,year, month))
    
    paylist = show('socialsecurityagency%s%s'%(year, month))[['worker_id','social_security_employer']]
    paylist.columns = ['worker_id','DR']
    paylist['ma_account_id'] = 'MA1222'
    paylist['in'] = 'Social security paid for external'
    paylist.set_index('in', inplace = True)
    returndf = pd.concat([paylist,
                       pd.DataFrame({'DR':[0],'CR':[paylist['DR'].sum()],
                                     'ma_account_id':['MA221104']
                                     }, 
                       index = ['Social security paid for external'])])

    returndf['date'] = year+'/'+month
    return returndf.fillna(0)
    
    

def processingpayroll(year, month):
    # Get payrol for one month
    
    paymentprocess(year, month)
    
    dfs = []
    dfs.append(
    payrollregular(year, month)
    )
    
    dfs.append(
    payrollassociate(year, month)
    )

    
    dfs.append(
    payrollexternal(year, month)
    )
    
    return pd.concat(dfs).fillna(0).reset_index().reindex([0,5,1,2,3,6,8,9,10,11,12])
    
