import sqlite3
import pandas as pd

con = sqlite3.connect('test01.db')

def show(table):
    return pd.read_sql_query("SELECT * FROM %s" %(table), con)

# globalsettings
account_receivable_code = "MA1122"
bfdatadate = "2020-12-31"

def create_view_machineday(start,end):
    # Count machine days
    con.execute(
    'DROP VIEW IF EXISTS machinedaycalculation;'
    )
    con.execute(
    """
    CREATE VIEW machinedaycalculation AS SELECT *, 
    MAX(JULIANDAY('%s') - JULIANDAY(date),0)*quantity as accumulated_machine_days_per_starting, 
    MAX(JULIANDAY('%s') - JULIANDAY(date),0)*quantity as accumulated_machine_days_per_ending
    FROM relocation;
    """ % (start , end)
    )
    
def create_view_machinedaysummary():
  # build a view to calculate machine*days for each q_id and equip_type_name

    con.execute(
    'DROP VIEW IF EXISTS machinedaysummary;'
    )

    con.execute(
    '''
    CREATE VIEW machinedaysummary AS 
    SELECT  q_id, qt_id, equip_type_name, equip_type_id,
    sum(accumulated_machine_days_per_starting) AS Accumulated_machine_days_BF, 
    sum(accumulated_machine_days_per_ending) AS Accumulated_machine_days_CF, sum(accumulated_machine_days_per_ending) - sum(accumulated_machine_days_per_starting) AS Machine_days_for_the_period   FROM machinedaycalculation GROUP BY q_id, equip_type_name;
    ''')
    
    
# calculating accrued revenue 
# based on the number of days that machines stay in location, and, 
# the term of the contracts

def create_view_accrued_revenue(start,end):
    
    create_view_machineday(start,end)
    create_view_machinedaysummary()
    
    # Temporarily split rent to every equipment
    con.execute(
    'DROP VIEW IF EXISTS vqsumofquantity;'
    )
    con.execute("""
    CREATE VIEW vqsumofquantity AS 
        SELECT 
            qt_id, 
            SUM(quantity) AS sumofquantity 
        FROM vq_equip 

        GROUP BY qt_id;
    """)

    # Quotation: Wholesale type 
    con.execute(
    'DROP VIEW IF EXISTS type0quotation;'
    )
    con.execute("""
    CREATE VIEW type0quotation AS 
        SELECT 
            q_id, 
            qt_id, 
            rent_daily 
        FROM vq 
        WHERE quotation_type = 0;

    """)

    # Quotation: per-machine-day type 
    con.execute(
    'DROP VIEW IF EXISTS type1quotation;'
    )
    con.execute("""
    CREATE VIEW type1quotation AS 
        SELECT 
            q_id, 
            qt_id,
            equip_quantity,
            equip_type_id,
            rent_daily 
        FROM vq 
        WHERE quotation_type = 1;

    """)

    # Quotation: per-machinegroup-day type 
    con.execute(
    'DROP VIEW IF EXISTS type2quotation;'
    )
    con.execute("""
    CREATE VIEW type2quotation AS 
        SELECT 
            q_id, 
            qt_id, 
            rent_daily 
        FROM vq 
        WHERE quotation_type = 2;

    """)
    
    #### Type 0: quotation is a single number cover  days (which can only be estimated)
    # For type 0 quotation, Only split revenue by time period

    con.execute(
    'DROP VIEW IF EXISTS revenue_type0;'
    )

    con.execute("""
    CREATE VIEW revenue_type0 AS 

    SELECT 
        q_id, 
        qt_id, 
        rent_daily/totaldays*days AS Revenue_for_the_period 
    FROM type0quotation

    LEFT JOIN

        (SELECT  qt_id AS handler, 
         - ( MAX(JULIANDAY('%s') - JULIANDAY(entry_date),0) - MAX(JULIANDAY('%s') - JULIANDAY(exit_date),0) ) + 
         ( MAX(JULIANDAY('%s') - JULIANDAY(entry_date),0) - MAX(JULIANDAY('%s') - JULIANDAY(exit_date),0) ) AS days,
         JULIANDAY(exit_date)-JULIANDAY(entry_date) AS totaldays
        FROM vq_wholesale )

    ON handler = type0quotation.qt_id

    """ % (start,start ,end, end)
    )
    
    # Type 1: quotation is by machine and by day
    # For type 1 quotation, calculate by quantity in TABLE vq directly.
    con.execute(
    'DROP VIEW IF EXISTS revenue_type1;'
    )
    con.execute('''
    CREATE VIEW revenue_type1 AS 
        SELECT 
            q_id,
            qt_id, 
            equip_type_id,
            equip_type_name, 
            unitprice*Machine_days_for_the_period AS Revenue_for_the_period 
        FROM 
            (SELECT 
                *, 
                rent_daily/equip_quantity AS unitprice 
             FROM type1quotation 
             LEFT JOIN machinedaysummary 
             ON type1quotation.equip_type_id = machinedaysummary.equip_type_id 
                 AND type1quotation.q_id = machinedaysummary.q_id
            )
    ''')
    
    
    # Type 2: quotation is by day with more than one equipment
    # For type 2 quotation, calculate by quantity in TABLE vq_equip.
    # Currently using simple method split daily revenue equally to different equipments. 
    # May need to adjust later on. 
    con.execute(
    'DROP VIEW IF EXISTS revenue_type2;'
    )

    con.execute('''
    CREATE VIEW revenue_type2 AS 

    SELECT 
        q_id,
        leftqt_id AS qt_id, 
        Combined_machine_days * unitprice AS Revenue_for_the_period 
    FROM (

            (SELECT 
                qt_id AS leftqt_id, 
                SUM(Machine_days_for_the_period) AS Combined_machine_days 
                FROM machinedaysummary 
                WHERE qt_id in (SELECT qt_id FROM type2quotation) GROUP BY qt_id)

            LEFT JOIN 

            (SELECT 
                type2quotation.q_id,
                type2quotation.qt_id AS rightqt_id, 
                rent_daily/sumofquantity AS unitprice 
             FROM type2quotation LEFT JOIN vqsumofquantity ON type2quotation.qt_id = vqsumofquantity.qt_id)

             ON leftqt_id = rightqt_id
         )
    ''')
    
    # build a view to combine all revenues from rent
    con.execute(
    'DROP VIEW IF EXISTS Revenue_from_rent;'
    )

    con.execute("""
    CREATE VIEW Revenue_from_rent AS

    SELECT q_id,qt_id,Revenue_for_the_period FROM revenue_type0
    UNION
    SELECT q_id,qt_id,Revenue_for_the_period FROM revenue_type1
    UNION
    SELECT q_id,qt_id,Revenue_for_the_period FROM revenue_type2

    """ 
    )
    
    
def show_accrued_revenue(start,end):
    create_view_accrued_revenue(start,end)
    
    return show('Revenue_from_rent')


# statistics of invoices
# invoiced revenue shall be the same as the revenue on tax returns
def create_view_invoiced_revenue(start, end):

    ### Invoice
    con.execute(
    'DROP VIEW IF EXISTS invoice_for_the_period;'
    )

    con.execute("""
    CREATE VIEW invoice_for_the_period AS

    SELECT 
        q_id, 
        client_id, 
        SUM(price) AS Invoiced_Amount, 
        SUM(vat) AS Invoiced_VAT, 
        SUM(price_vat_total) AS Invoiced_Total 
    FROM invoice 
    WHERE JULIANDAY(date)>JULIANDAY('%s') 
        AND JULIANDAY(date)<JULIANDAY('%s') 
    GROUP BY q_id,  Client_id
    """ %(start, end))
    
def show_invoiced_revenue(start, end):
    create_view_invoiced_revenue(start, end)
    return show('invoice_for_the_period')

  
def create_view_standardcost(start,end):
  
    create_view_machineday(start,end)
    create_view_machinedaysummary()
  
    con.execute(
    'DROP VIEW IF EXISTS Standard_machine_cost_by_quotation_and_machine;'
    )

    con.execute("""
    CREATE VIEW Standard_machine_cost_by_quotation_and_machine AS

        SELECT 
            q_id, 
            machinedaysummary.equip_type_name, 
            machinedaysummary.equip_type_id,
            cost_day_std * Machine_days_for_the_period AS Standard_machine_cost_for_the_period 
        FROM machinedaysummary

        LEFT JOIN  equip_dailycost 
            ON machinedaysummary.equip_type_name = equip_dailycost.equip_type_name;
    """)
    
    # Calculate per-quotation&machine cost 

    con.execute(
    'DROP VIEW IF EXISTS Standard_machine_cost_by_quotation_and_machine;'
    )

    con.execute("""
    CREATE VIEW Standard_machine_cost_by_quotation_and_machine AS

        SELECT 
            q_id, 
            machinedaysummary.equip_type_name, 
            machinedaysummary.equip_type_id,
            cost_day_std * Machi               ne_days_for_the_period AS Standard_machine_cost_for_the_period 
        FROM machinedaysummary

        LEFT JOIN  equip_dailycost 
            ON machinedaysummary.equip_type_name = equip_dailycost.equip_type_name;
    """)
    
    # Calculate per-quotation cost 
    con.execute(
    'DROP VIEW IF EXISTS Standard_machine_cost_by_quotation;'
    )
    
    con.execute('''
    CREATE VIEW Standard_machine_cost_by_quotation AS

        SELECT 
            q_id, 
            SUM(Standard_Machine_cost_for_the_period) AS Standard_machine_cost_for_the_period 
        FROM Standard_machine_cost_by_quotation_and_machine 

        GROUP BY q_id;
    ''')
def show_standardcost(start,end):
    create_view_standardcost(start,end)
    return show('Standard_machine_cost_by_quotation')

def create_view_contract_profitability(start,end):
    
    create_view_accrued_revenue(start,end)
    create_view_standardcost(start,end)
    
    ### Client revenue and cost
    con.execute(
    'DROP VIEW IF EXISTS ContractGrossProfit;'
    )

    con.execute("""
    CREATE VIEW ContractGrossProfit AS

    SELECT 
        q_id, 
        ep_id, 
        ep_name, 
        Revenue, 
        Standard_machine_cost_for_the_period AS StdCost, 
        Revenue- Standard_machine_cost_for_the_period AS GrossProfit  

    FROM 
        (
            (
                (
                SELECT 
                    q_id AS leftid, 
                    SUM(Revenue_for_the_period) 
                    AS Revenue 
                FROM Revenue_from_rent GROUP BY q_id
                )

                LEFT JOIN  
                    Standard_machine_cost_by_quotation 
                    ON leftid = Standard_machine_cost_by_quotation.q_id 
                )

            LEFT JOIN 

            (SELECT 
                DISTINCT(q_id) AS rightid, 
                ep_id, 
                ep_name FROM vq) 

            ON leftid = rightid
        )
    """)
    
def show_contract_profitability(start,end):
    create_view_contract_profitability(start,end)
    return show('ContractGrossProfit')



def create_view_contract_recovery(start,end):
    ### clearing
    con.execute(
    'DROP VIEW IF EXISTS receipt_for_the_period_by_client;'
    )

    con.execute("""
    CREATE VIEW receipt_for_the_period_by_client AS

    SELECT 
        ep_id, 
        ep_name, 
        SUM(IIF(is_cancelling = NULL, -1,1)*amount) AS Received_amount
    FROM bankstatement 
    WHERE ma_account_id = "%s"
        AND JULIANDAY(date)>JULIANDAY("%s") 
        AND JULIANDAY(date)<JULIANDAY("%s") 
    GROUP BY ep_id, ep_name
    """ %(account_receivable_code, start, end))
    
def show_contract_recovery(start,end):
    create_view_contract_recovery(start,end)
    return show('receipt_for_the_period_by_client')

def create_view_ClientGrossProfit(start,end):

    create_view_contract_profitability(start,end)

    ### Client revenue and cost
    con.execute(
    'DROP VIEW IF EXISTS ClientGrossProfit;'
    )

    con.execute("""
    CREATE VIEW ClientGrossProfit AS

    SELECT 
        ep_id,
        ep_name,
        SUM(Revenue) AS Revenue,
        SUM(StdCost) AS StdCost,
        SUM(GrossProfit) AS GrossProfit
    FROM 
        ContractGrossProfit
    GROUP BY
        ep_id
    """)
    
def show_ClientGrossProfit(start,end):
    create_view_ClientGrossProfit(start,end)
    return show('ClientGrossProfit')

def create_view_ClientRecovery(start,end):

    # Created a view to show all client id and the balance as at 2020-12-31

    con.execute(
    'DROP VIEW IF EXISTS client_bf;'
    )

    con.execute("""
    CREATE VIEW client_bf AS

        SELECT * FROM 

        ( SELECT ep_id FROM external_parties where ep_type = 'client' )

        LEFT JOIN     (
            SELECT ep_id AS bf_id , amount FROM ep_bf 
            WHERE ma_account_id = '%s'
            ) 
        ON ep_id = bf_id

    """%(account_receivable_code))
    
    viewname = end[5:7]

    con.execute(
    'DROP VIEW IF EXISTS client_per_%s;'%(viewname)
    )

    con.execute("""
    CREATE VIEW client_per_%s AS

    SELECT ep_id, amount - Recevied_amount + Invoiced_Total AS cutdateamount  FROM
        (
        /* Use original point amout deduct recieved and add invoiced to (continued)*/
        /* reach the bring/forward amount as at the starting date */
            (
            SELECT * FROM client_bf  

            /* Get the 2020 data as original point*/

                LEFT JOIN 
                    ( 

            /* Find the amount received from the client between original point to staring date*/

                    SELECT ep_id as receipt_ep_id, 
                            sum(amount) AS Recevied_amount

                            FROM bankstatement 

                            WHERE JULIANDAY(date)>JULIANDAY("%s") 
                            AND  JULIANDAY(date)<=JULIANDAY("%s") 
                                    AND ma_account_id = '%s'
                            GROUP BY ep_id
                    ) 
                ON ep_id = receipt_ep_id

            LEFT JOIN 
                (SELECT 
                 /* Find the amount invoiced the client between original point to staring date*/
                    q_id, 
                    client_id, 
                    SUM(price) AS Invoiced_Amount, 
                    SUM(vat) AS Invoiced_VAT, 
                    SUM(price_vat_total) AS Invoiced_Total 
                FROM invoice 
                WHERE JULIANDAY(date)>JULIANDAY("%s") 
                    AND JULIANDAY(date)<=JULIANDAY("%s") 
                    GROUP BY q_id,  Client_id
                )
            ON ep_id = client_id
            )
        )

    """%(viewname, bfdatadate,start, account_receivable_code, bfdatadate,start))

    con.execute(
    'DROP VIEW IF EXISTS client_Recovery_report;'
    )

    con.execute("""
    CREATE VIEW client_Recovery_report AS

        SELECT * FROM

        (
        SELECT ep_id, cutdateamount AS Opening,  invoiced AS Invoiced, receipt AS receiving, IFNULL(cutdateamount,0)+IFNULL(invoiced,0)-IFNULL(receipt,0) AS Closing 
         FROM client_per_%s

        LEFT JOIN 
        (
        SELECT ep_id as receipt_ep_id, 
                    sum(amount) AS receipt

                    FROM bankstatement 

                    WHERE JULIANDAY(date)>JULIANDAY("%s") 
                    AND  JULIANDAY(date)<=JULIANDAY("%s") 
                            AND ma_account_id = '%s'
                    GROUP BY ep_id
        )
        ON ep_id = receipt_ep_id

        LEFT JOIN 
            (SELECT 

                    client_id, 

                    SUM(price_vat_total) AS invoiced 
                FROM invoice 
                WHERE JULIANDAY(date)>JULIANDAY("%s") 
                    AND JULIANDAY(date)<=JULIANDAY("%s") 
                    GROUP BY q_id,  Client_id)
        ON ep_id = client_id
        )

        WHERE NOT (Opening IS NULL AND Invoiced IS NULL AND Receiving IS NULL)

    """%(viewname, start, end,account_receivable_code, start, end))
    
def show_ClientRecovery(start,end):
    create_view_ClientRecovery(start,end)
    return show('client_Recovery_report').fillna(0)

def create_view_ContractClearing(start, end):
    
    
    create_view_contract_recovery(start, end)

    ### Combined, still need opening balance 
    con.execute(
    'DROP VIEW IF EXISTS Client_clearing;'
    )

    con.execute("""
    CREATE VIEW Client_clearing AS

    SELECT 
        ep_id,
        ep_name, 
        Received_amount, 
        Invoiced_Total 
    FROM receipt_for_the_period_by_client 
    JOIN invoice_for_the_period 
    ON receipt_for_the_period_by_client.ep_id = invoice_for_the_period.client_id
    """)
    
def show_ContractClearing(start, end):
    create_view_ContractClearing(start, end)
    
    return show('Client_clearing')

