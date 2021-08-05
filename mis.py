import sqlite3 
import pandas as pd

# globalsettings


class mis:

    account_receivable_code = "MA1122"
    bfdatadate = "2020-12-31"

    con = ''

# Initiate
    def __init__(self):
        pass

    def convert(self, schedulefile, dbfile):
        try:
            sourcefile = pd.read_excel(schedulefile+'.xlsx', sheet_name=None)
        except:
            print ('Cannot open sourcefile')

        try:
            self.con = sqlite3.connect(dbfile)

            for sheetname in sourcefile:
                sourcefile[sheetname].to_sql(sheetname, self.con)
            self.con.close()
        except:
            print ('Cannot write to database.')

    def connect(self, dbfile):
        try: 
            self.con.close()
        except:
            pass

        try:
            self.con = sqlite3.connect(dbfile)
        except:
            print ('Cannot connect to database.')

    def show(self, tablename):
        return pd.read_sql_query("SELECT * FROM %s" %(tablename), self.con)


## data integrity

    def check_relocation_nagative(self):
        
        df = self.show('relocation')
        
        df['dquantity'] = df['quantity']*df['direction']

        for date in pd.unique(df['date']):
            slicedf = df[df['date']<= date].copy()

            if (int(slicedf.groupby(['q_id','site_name','equip_type_name']).sum()[['dquantity']].min())<0):
                print ('error'+str(date))
                return date
        return 0

    def check_bf_account_vs_externalparties(self):

        self.con.execute(
        'DROP VIEW IF EXISTS ReconcileEPBF ;'
        )

        self.con.execute("""
        CREATE VIEW ReconcileEPBF AS

        SELECT ma_account_id, bf_amount - EPSUM AS BFDIFF FROM (SELECT ma_account_id AS MID, SUM(amount) AS EPSUM FROM ep_bf GROUP BY ma_account_id)
        LEFT JOIN (SELECT bf_amount, ma_account_id FROM account_bf) on MID = ma_account_id
        """)

        return self.show('ReconcileEPBF')

    def check_bankstatement_for_missing_ep_id(self):
        self.con.execute(
        'DROP VIEW IF EXISTS invalid_ep_id_bank ;'
        )

        self.con.execute("""
        CREATE VIEW invalid_ep_id_bank AS

        SELECT * FROM (SELECT ma_account_id AS MID FROM chart_of_accounts WHERE AP_AR = 1)
        LEFT JOIN ( SELECT ma_account_id AS MID2, ep_id, amount FROM bankstatement) 
        ON MID = MID2 WHERE NOT MID2 IS NULL AND ep_id IS NULL
        """)
        return self.show('invalid_ep_id_bank')
        

    def check_AJE_for_missing_ep_id(self):

        self.con.execute(
        'DROP VIEW IF EXISTS invalid_ep_id_AJE ;'
        )

        self.con.execute("""
        CREATE VIEW invalid_ep_id_AJE AS

        SELECT * FROM (SELECT ma_account_id AS MID FROM chart_of_accounts WHERE AP_AR = 1)
        LEFT JOIN ( SELECT ma_account_id AS MID2, ep_id, amount FROM AJE) 
        ON MID = MID2 WHERE NOT MID2 IS NULL AND ep_id IS NULL
        """)

        return self.show('invalid_ep_id_AJE')

    def check_missing_VAT(self):
        return self.show("bankstatement WHERE VAT IS NULL")






# Contract management
    def create_view_machineday(self, start,end):
        # Count machine days
        self.con.execute(
        'DROP VIEW IF EXISTS machinedaycalculation;'
        )
        self.con.execute(
        """
        CREATE VIEW machinedaycalculation AS SELECT *, 
        MAX(JULIANDAY('%s') - JULIANDAY(date),0)*quantity as accumulated_machine_days_per_starting, 
        MAX(JULIANDAY('%s') - JULIANDAY(date),0)*quantity as accumulated_machine_days_per_ending
        FROM relocation;
        """ % (start , end)
        )
        
    def create_view_machinedaysummary(self ):
    # build a view to calculate machine*days for each q_id and equip_type_name

        self.con.execute(
        'DROP VIEW IF EXISTS machinedaysummary;'
        )

        self.con.execute(
        '''
        CREATE VIEW machinedaysummary AS 
        SELECT  q_id, qt_id, equip_type_name, equip_type_id,
        sum(accumulated_machine_days_per_starting) AS Accumulated_machine_days_BF, 
        sum(accumulated_machine_days_per_ending) AS Accumulated_machine_days_CF, sum(accumulated_machine_days_per_ending) - sum(accumulated_machine_days_per_starting) AS Machine_days_for_the_period   FROM machinedaycalculation GROUP BY q_id, equip_type_name;
        ''')
        
        
    # calculating accrued revenue 
    # based on the number of days that machines stay in location, and, 
    # the term of the self.contracts

    def create_view_accrued_revenue(self, start,end):
        
        self.create_view_machineday(start,end)
        self.create_view_machinedaysummary()
        
        # Temporarily split rent to every equipment
        self.con.execute(
        'DROP VIEW IF EXISTS vqsumofquantity;'
        )
        self.con.execute("""
        CREATE VIEW vqsumofquantity AS 
            SELECT 
                qt_id, 
                SUM(quantity) AS sumofquantity 
            FROM vq_equip 

            GROUP BY qt_id;
        """)

        # Quotation: Wholesale type 
        self.con.execute(
        'DROP VIEW IF EXISTS type0quotation;'
        )
        self.con.execute("""
        CREATE VIEW type0quotation AS 
            SELECT 
                q_id, 
                qt_id, 
                rent_daily 
            FROM vq 
            WHERE quotation_type = 0;

        """)

        # Quotation: per-machine-day type 
        self.con.execute(
        'DROP VIEW IF EXISTS type1quotation;'
        )
        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS type2quotation;'
        )
        self.con.execute("""
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

        self.con.execute(
        'DROP VIEW IF EXISTS revenue_type0;'
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS revenue_type1;'
        )
        self.con.execute('''
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
        self.con.execute(
        'DROP VIEW IF EXISTS revenue_type2;'
        )

        self.con.execute('''
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
        self.con.execute(
        'DROP VIEW IF EXISTS Revenue_from_rent;'
        )

        self.con.execute("""
        CREATE VIEW Revenue_from_rent AS

        SELECT q_id,qt_id,Revenue_for_the_period FROM revenue_type0
        UNION
        SELECT q_id,qt_id,Revenue_for_the_period FROM revenue_type1
        UNION
        SELECT q_id,qt_id,Revenue_for_the_period FROM revenue_type2

        """ 
        )
        
        
    def show_accrued_revenue(self, start,end):
        self.create_view_accrued_revenue(start,end)
        
        return self.show('Revenue_from_rent')


    # statistics of invoices
    # invoiced revenue shall be the same as the revenue on tax returns
    def create_view_invoiced_revenue(self, start, end):

        ### Invoice
        self.con.execute(
        'DROP VIEW IF EXISTS invoice_for_the_period;'
        )

        self.con.execute("""
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
        
    def show_invoiced_revenue(self, start, end):
        self.create_view_invoiced_revenue(start, end)
        return self.show('invoice_for_the_period')

    
    def create_view_standardcost(self, start,end):
    
        self.create_view_machineday(start,end)
        self.create_view_machinedaysummary()
    
        self.con.execute(
        'DROP VIEW IF EXISTS Standard_machine_cost_by_quotation_and_machine;'
        )

        self.con.execute("""
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

        self.con.execute(
        'DROP VIEW IF EXISTS Standard_machine_cost_by_quotation_and_machine;'
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS Standard_machine_cost_by_quotation;'
        )
        
        self.con.execute('''
        CREATE VIEW Standard_machine_cost_by_quotation AS

            SELECT 
                q_id, 
                SUM(Standard_Machine_cost_for_the_period) AS Standard_machine_cost_for_the_period 
            FROM Standard_machine_cost_by_quotation_and_machine 

            GROUP BY q_id;
        ''')
    def show_standardcost(self, start,end):
        self.create_view_standardcost(start,end)
        return self.show('Standard_machine_cost_by_quotation')

    def create_view_contract_profitability(self, start,end):
        
        self.create_view_accrued_revenue(start,end)
        self.create_view_standardcost(start,end)
        
        ### Client revenue and cost
        self.con.execute(
        'DROP VIEW IF EXISTS ContractGrossProfit;'
        )

        self.con.execute("""
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
        
    def show_contract_profitability(self, start,end):
        self.create_view_contract_profitability(start,end)
        return self.show('ContractGrossProfit')



    def create_view_contract_recovery(self, start,end):
        ### clearing
        self.con.execute(
        'DROP VIEW IF EXISTS receipt_for_the_period_by_client;'
        )

        self.con.execute("""
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
        """ %(self.account_receivable_code, start, end))
        
    def show_contract_recovery(self, start,end):
        self.create_view_contract_recovery(start,end)
        return self.show('receipt_for_the_period_by_client')

    def create_view_ClientGrossProfit(self, start,end):

        self.create_view_contract_profitability(start,end)

        ### Client revenue and cost
        self.con.execute(
        'DROP VIEW IF EXISTS ClientGrossProfit;'
        )

        self.con.execute("""
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
        
    def show_ClientGrossProfit(self, start,end):
        self.create_view_ClientGrossProfit(start,end)
        return self.show('ClientGrossProfit')

    def create_view_ClientRecovery(self, start,end):

        # Created a view to show all client id and the balance as at 2020-12-31

        self.con.execute(
        'DROP VIEW IF EXISTS client_bf;'
        )

        self.con.execute("""
        CREATE VIEW client_bf AS

            SELECT * FROM 

            ( SELECT ep_id FROM external_parties where ep_type = 'client' )

            LEFT JOIN     (
                SELECT ep_id AS bf_id , amount FROM ep_bf 
                WHERE ma_account_id = '%s'
                ) 
            ON ep_id = bf_id

        """%(self.account_receivable_code))
        
        viewname = end[5:7]

        self.con.execute(
        'DROP VIEW IF EXISTS client_per_%s;'%(viewname)
        )

        self.con.execute("""
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

        """%(viewname, self.bfdatadate,start, self.account_receivable_code, self.bfdatadate,start))

        self.con.execute(
        'DROP VIEW IF EXISTS client_Recovery_report;'
        )

        self.con.execute("""
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

        """%(viewname, start, end,self.account_receivable_code, start, end))
        
    def show_ClientRecovery(self, start,end):
        self.create_view_ClientRecovery(start,end)
        return self.show('client_Recovery_report').fillna(0)

    def create_view_ContractClearing(self, start, end):
        
        
        self.create_view_contract_recovery(start, end)

        ### Combined, still need opening balance 
        self.con.execute(
        'DROP VIEW IF EXISTS Client_clearing;'
        )

        self.con.execute("""
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
        
    def show_ContractClearing(self, start, end):
        self.create_view_ContractClearing(start, end)
        
        return self.show('Client_clearing')

    ## Financial reporting

    def create_view_fp_as_at(self, viewname,cuttingdate):
        
        bf_date = self.bfdatadate
        
        # (Query Alfa0): 
        # Notice the amount is negative because the amount is recorded as transaction of bank
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_nonbank_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_vat0_for_%s;'%(viewname)
        )
        
        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_vat1_for_%s;'%(viewname)
        )
        
        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_bank_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_ADJ_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS balance_at_%s;'%(viewname)
        )

        self.con.execute("""
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
        
    def show_fp_as_at(self, cuttingdate):    
        self.create_view_fp_as_at('frtemp',cuttingdate)
        return self.show("balance_at_frtemp")
        

    # Get duration data for income statement

    def create_view_pl_for(self, viewname, starting, ending):

        # (Query Alfa0: 
        # Notice the amount is negative because the amount is recorded as transaction of bank
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_nonbank_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_vat0_for_%s;'%(viewname)
        )
        
        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_vat1_for_%s;'%(viewname)
        )
        
        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_bank_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_banktransaction_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS accumulated_ADJ_for_%s;'%(viewname)
        )

        self.con.execute("""
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
        self.con.execute(
        'DROP VIEW IF EXISTS during%s;'%(viewname)
        )

        self.con.execute("""
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
        
        return self.show('during'+viewname)

    def show_pl_for(self,starting, ending):    
        self.create_view_pl_for("pltemp", starting, ending)

        return self.show('duringpltemp')


    # create a view for the cashflow between two dates for cashflow reports
    def create_view_cf_for(self, viewname, starting, ending):

        self.con.execute(
        'DROP VIEW IF EXISTS CF_for_%s;'%(viewname)
        )

        self.con.execute("""
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
    def show_cf_for(self, starting, ending):    
        self.create_view_cf_for("cftemp", starting, ending)
        return self.show("CF_for_cftemp")

    # Compile data into financial reports according to the template and mapping
    # A template is the format of a report. From each item to the final report
    # A mapping is the mapping from each account to each item
    # Template and mapping files are provided in this repository as sample csv files,
    # so that the reports can be changed via the csv files, without change the python code.
    
    def statementpreparing(self, reporttemplate, mapping, inputdf, reporttype, title):
        
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

    fpmapping = pd.DataFrame()
    plmapping = pd.DataFrame()
    cfmapping = pd.DataFrame()
    pltemplate = pd.DataFrame()
    fptemplate = pd.DataFrame()
    cftemplate = pd.DataFrame()

    def loading_FR_format(self,formatpath,formatname,mappingname):
        

        frmapping = pd.read_csv(formatpath+formatname)
        self.fpmapping = frmapping[frmapping['FP']==1]
        self.plmapping = frmapping[frmapping['PL']==1]
        # The line number shall be adjusted according to the csv file

        self.cfmapping = frmapping[frmapping['CF']==1]
        # The line number shall be adjusted according to the csv file
        self.cfmapping.columns = ['cf_account_id', 'cf_account_name', 'FP', 'PL', 'CF']
        self.cfmapping['cf_account_id'] = self.cfmapping['cf_account_id'].astype(int)

        frtemplate = pd.read_csv(formatpath+mappingname)
        self.pltemplate = frtemplate[~frtemplate['FP'].isnull()]
        self.fptemplate = frtemplate[~frtemplate['PL'].isnull()]
        self.cftemplate = frtemplate[~frtemplate['CF'].isnull()]

    def compileFR(self, lastreportingdate, numberofperiods, frequency):
        # Setting up the date parameter
        cuttingdates = pd.date_range(lastreportingdate, periods=numberofperiods+1, freq=frequency)

        # Process reports for each dates
        fp = []
        pl = []
        cf = []

        for closing in cuttingdates.astype(str):
            
            fp.append(
                self.statementpreparing(
                    self.fptemplate, 
                    self.fpmapping, 
                    self.show_fp_as_at(closing[5:7],closing), 
                    'FP', 
                    closing[5:7]
                    ).set_index('Report')    
            )
            
            try:
                pl.append(
                    
                    self.statementpreparing(
                        self.pltemplate, 
                        self.plmapping, 
                        self.show_pl_for(closing[5:7],opening,closing), 
                        'PL', 
                        closing[5:7]
                        ).set_index('Report')
                    )
                
                cf.append(
                    
                    self.statementpreparing(
                        self.cftemplate, 
                        self.cfmapping, 
                        self.show_cf_for(closing[5:7],opening,closing), 
                        'CF', 
                        closing[5:7]
                        ).set_index('Report')
                    )
            except:
                pass
            
            
            opening = closing

        # Combine reports into one dataframe for each type of reports.
        fps = pd.concat(fp, axis = 1)
        pls = pd.concat(pl, axis = 1)
        cfs = pd.concat(cf, axis = 1)

        return (fps,pls,cfs)



    # Payroll

    def create_view_paymentprocess(self, year, month):

        self.con.execute(
        'DROP VIEW IF EXISTS payroll%s%s;'%(year, month)
        )

        self.con.execute("""
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

    def payrollregular(self, year, month):
        

        self.create_view_paymentprocess(year, month)
        
        # Processing part 1: regular employees
        self.con.execute(
        'DROP VIEW IF EXISTS payrollinternal%s%s;'%(year, month)
        )
        self.con.execute("""
        CREATE VIEW payrollinternal%s%s AS

        SELECT left_worker_id AS worker_id, 
            total_salary, individual_income_tax
        , social_security_employee, house_tax,
            social_security_employer,
            department
        FROM payroll%s%s

        WHERE Social_security_recoverable IS NULL
        """%(year, month, year, month))
        
        regtdf = self.show('payrollinternal%s%s'%(year, month)).groupby('department').sum()
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

    def payrollassociate(self, year, month):

        self.con.execute(
        'DROP VIEW IF EXISTS payrollexternal%s%s;'%(year, month)
        )
        self.con.execute("""
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
        
        sssum = self.show('payrollexternal%s%s'%(year, month))['social_security_employer'].sum()
        return pd.DataFrame({'DR':[sssum,0], 
                            'CR':[0,sssum],
                            'ma_account_id':['MA560234','MA221104'],
                            'date':[year+'/'+month,year+'/'+month]}, 
                            index = ['Social security paid for rp','Social security paid for rp'])

    def payrollexternal(self, year, month):
        self.create_view_paymentprocess(year, month)
        
        self.con.execute(
        'DROP VIEW IF EXISTS socialsecurityagency%s%s;'%(year, month)
        )
        self.con.execute("""
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
        
        paylist = self.show('socialsecurityagency%s%s'%(year, month))[['worker_id','social_security_employer']]
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
        

    def processingpayroll(self, year, month):
        # Get payrol for one month
        
        self.create_view_paymentprocess(year, month)
        
        dfs = [
            self.payrollregular(year, month),
            self.payrollassociate(year, month),
            self.payrollexternal(year, month)
            ]

        return pd.concat(dfs).fillna(0).reset_index().reindex([0,5,1,2,3,6,8,9,10,11,12])
        
