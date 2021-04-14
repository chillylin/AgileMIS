#!/usr/bin/env python
# coding: utf-8

# In[1]:


import pandas as pd


# In[2]:


path  = ''
def loadfile(filename):
    return pd.read_excel(path+filename+'.xlsx', sheet_name=None)


# In[155]:


dfdict = loadfile('schedules')


# In[4]:


for sheetname in dfdict:
    print (sheetname)


# # Equipment cost measured with equipment day

# In[5]:


relocation = dfdict['relocation'].copy()
relocation


# In[ ]:


# Confirming no nagative 


# In[ ]:


*temp['direction']


# In[ ]:


def testnagative(df):
    
    df['dquantity'] = df['quantity']*df['direction']

    for date in pd.unique(df['date']):
        slicedf = df[df['date']<= date].copy()

        if (int(slicedf.groupby(['q_id','site_name','equip_type_name']).sum()[['dquantity']].min())<0):
            print ('error'+str(date))
            return date
    return 0


# In[ ]:


testnagative(relocation)


# In[207]:


def daycount(df,datetext):
    
    df['dateacc'] =  (pd.to_datetime(datetext)-df['date']).dt.days.clip(lower=0)
    df['dateaccd'] = df['dateacc']*df['direction']*df['quantity']
    returningdf = df.groupby(['q_id','site_name','equip_type_name']).sum()[['dateaccd']]
    
    return returningdf[returningdf['dateaccd']>0]

def perioddaycount(df,start,end):
    
    return (daycount(df,end)-daycount(df,start)).dropna()


# # Standard cost

# In[65]:


dailycost = dfdict['equip_dailycost'].dropna().set_index('equip_type_name')


# In[204]:


def periodstandardcost(locationdf,standardcostdf,start,end, costestimation = 'cost_day_std'):
    stdcostdf = perioddaycount(locationdf,start,end).join(standardcostdf).dropna()
    
    
    stdcostdf['standard_cost'] = stdcostdf['dateaccd']*stdcostdf[costestimation]
    
    return stdcostdf[stdcostdf['standard_cost']>0][['standard_cost']]


# In[208]:


periodstandardcost(relocation, dailycost, '2021-01-01','2021-02-01','cost_day_high')


# In[206]:





# In[ ]:





# # Accrual revenue

# In[ ]:


# assuming the price is combined price


# In[11]:


vq = dfdict['vq']
vq_equip = dfdict['vq_equip']


# In[247]:


def periodrevenue(locationdf, visual_query, visual_query_equipment, start, end):
    
    rentdf = visual_query[['q_id','qt_id','rent_daily']].join(
        visual_query_equipment[['qt_id','quantity']].groupby('qt_id').sum(), 
        on = 'qt_id')
    
    rentdf['unit_rent'] = rentdf['rent_daily']/rentdf['quantity']

    
    revenuedf = perioddaycount(locationdf, start, end). join(
    vq_equip[['qt_id','equip_type_name']].join(
        rentdf.set_index('qt_id'), 
        on ='qt_id')[['q_id','equip_type_name','unit_rent','qt_id'
                     ]].set_index(['q_id','qt_id','equip_type_name'])
    )
    
    revenuedf['q_period_revenue'] = revenuedf['dateaccd']*revenuedf['unit_rent']
    
    return revenuedf[['q_period_revenue']]


# In[246]:





# # Client value

# In[228]:


def clientvalue(location,visual_query,visual_query_equipment, dailycost, start,end,costing = 'cost_day_std'):

    df = pd.concat([
        # Combining revenue table, and,
        periodrevenue(
            location,visual_query,visual_query_equipment,start,end
        ).reset_index().groupby('q_id').sum(),
        
        # standard cost table
        periodstandardcost(
            location, dailycost, start,end
        ).reset_index().groupby('q_id').sum(),
        
        ], axis = 1).dropna(
    ).join(
        # Then added external party lable
        vq[['q_id','ep_id']].drop_duplicates().set_index('q_id')
    ).reset_index(
        # And accumulate numbers based on exetnal party
    ).groupby('ep_id').sum()
    
    # calculate indicators for client value
    df['gross_margin'] = df['q_period_revenue']-df['standard_cost']
    df['gross_margin_rate'] = df['gross_margin']/ df['q_period_revenue']
    
    return df


# In[229]:


clientvalue(relocation,vq,vq_equip,dailycost,'2021-01-01','2021-02-01')


# In[200]:


periodrevenue(
            relocation,vq,vq_equip,'2021-01-01','2021-02-01'
        )


# In[79]:





# In[80]:



df


# # Contract Clearing

# In[60]:


invoice = dfdict['invoice']
bank = dfdict['bankstatement']


# In[135]:


def period_invoice(df,start,end):
      
    return df[(df['date']>pd.to_datetime(start)) &
              (df['date']<pd.to_datetime(end))
             ][['q_id','client_id','price','vat']
              ].groupby(['q_id','client_id']).sum()

def period_bank(df,start,end):
      
    return df[(df['date']>pd.to_datetime(start))&
              (df['date']<pd.to_datetime(end))].copy()

def getexternalpartytransaction(bankdf, start, end):

    df = period_bank(bankdf,start, end)
    flag = df['is_cancelling'].replace({1:-1}).fillna(1)
    df['amount_final'] = df['amount']*flag
    df['vat_final'] = df['vat']*flag
    
    return df[['ep_id','ma_account_id','amount_final'
              ]].groupby(['ma_account_id','ep_id']
                        ).sum().reset_index()

def getledger(df,ma_account_id):
    return df[df['ma_account_id']==ma_account_id]

def getcontractclearing(invoice,bank,start,end):
    return period_invoice(invoice,start,end).join(
        getledger(getexternalpartytransaction(bank,start,end),5001).set_index('ep_id')['amount_final'],
        on = 'client_id'
    ).fillna(0)


# In[193]:


getcontractclearing(invoice,bank,'1990-01-01','2021-03-01')


# # PM of the sales

# In[234]:


qwmapping = dfdict['quot_worker_mapping']


# In[298]:


def salespm(location,visual_query,visual_query_equipment, dailycost, start,end,costing = 'cost_day_std'):

    df = periodrevenue(relocation,vq,vq_equip,'2021-01-01','2021-02-01').join(qwmapping[['qt_id','worker_id','percent']].set_index('qt_id'))
    
    
    df = df.join(periodstandardcost(
            location, dailycost, start,end
        )).fillna(0)
    
    df['revenueforperson'] = df['q_period_revenue']*df['percent']
    df['costforperson'] = df['standard_cost']*df['percent']
    
    returningdf = df[['worker_id','revenueforperson','costforperson']].groupby('worker_id').sum()
    # calculate indicators for client value
    returningdf['gross_margin'] = returningdf['revenueforperson']-returningdf['costforperson']
    returningdf['gross_margin_rate'] = returningdf['gross_margin']/ returningdf['revenueforperson']
    
    return returningdf


# In[301]:


salespm(relocation,vq,vq_equip,dailycost,'2021-01-01','2021-02-01')


# # Financial reports

# In[310]:


bank[['ma_account_id','ma_account','amount']].groupby(['ma_account_id','ma_account']).sum()


# In[ ]:


df[['worker_id','revenueforperson']].groupby('worker_id').sum()


# In[307]:


bank


# In[236]:





# In[ ]:





# In[233]:


temp.join(
        # Then added external party lable
        vq[['qt_id','ep_id']].drop_duplicates().set_index('q_id')
    ).reset_index(
        # And accumulate numbers based on exetnal party
    ).groupby('ep_id').sum()


# In[ ]:




