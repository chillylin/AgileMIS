# Aim

This repo aims to provide a record for the concept that MIS shall be built with the expectation for the ever changing need of the management. 

# Background
THis MIS is built for a rental company.
The company had three major department: sales, post-sale, and, the admin. The sales seek for potential client and were incentivised according to the profitability and rent collection. The post-sales controls the rental process by sending out and retrieving the rented equipment. The admin does the rest. 

There are three major stakeholders in this project: the CEO, the financial manager, and the advisor. 
1. The CEO, also the chief salesperson, currently had a manual management accounting record based on his need. 
1. The financial manager monitors the outsourced compliance accounting for taxation.  The manager also maintained an inhouse cash budget system manually. Every month, the manager manually adjust the report form compliance accounting for management use. 
1. The advisor is responsible for helping the CEO and financial manager in making business decisions including but not limited to cash flow planning,  structure design and efficiency improving. The advisor had experience in Python and data analytics but is not a developer for cross-platform applications. 

# Stage 1: initialising the project

CEO asked to buy a system that can manage the equipment rented out. The approach is "finding a on-the-shelf software and test what it can do", instead of "thinking what is needed, and, then find the way to achieve the demand". 

Advisor suggested to clarify the demand before the decision of purchase. 

# Stage 2: Demand identification

Advisor found that the daily job of preparing cash budget (along with actual figure) and monthly report could be automated. 
Also, the demand is not collected because different department did not cooperated, possibly due to an unclear expectation for the software. 

Therefore, the advisor decided to build a management information system that can fit the need for the cash budget, monthly management report and the management report can coincidently solve the problem of equipment management. 

The decision of making the system in-house is because the normal compliance accounting service is already built by outsider financial report compilers. Thus, the in-house system does need to be of exceptional quality but instead the agility of such system is preferred. 

# stage 3: Considerations on developing the prototype
The advisor discussed with the financial manager and the CEO about the demand of such MIS before an schedule of all information input is agreed upon by the financial manager and the advisor. The financial manager agreed to collect the information for such a schedule. 

The template of the schedule is in MS Excel format for the financial manager to fill. 

## Choosing interface
The interface was contemplated. Current expectation is a system running at local machine since the MIS aims to reduce the job load of the financial manager. However, remote access could provide two benefits: 1. allow the CEO can access the system for data needs  2. allow sales and post-sales engineers to access the system to input data. 

1. A local executable: Good to control user experience but the quality of such experience could not be expected since the advisor cannot waste time on UI design and the UI design is not the core demand for the system.
1. PHP webpage rendered by XAMPP: Easy to implement and could allow remote engineers to login, retrieving and inserting information about equipment; however, the user interface could be rather ugly.
1. Excel: Using excel for data input and output could only be used at local machine. However, Excel is well designed and optimised for user experience. Also, the cost in UI would be almost none. 
1. Other development kit for local machines: Could be complex in adapting the OS of different clients, especially considering the remote users may need to run the system on iPhone or highly altered Android devices. 

Therefore, Excel method is chosen during the prototyping process. 

## Choosing language

If the MIS was deemed to be running on a ongoing basis with few changes, C++ could be the best choice for developing the system. However, since the system was expected to be agile, fulfilling the changing need for the management accounting, a more easy-to-use language shall be preferred. The easy-to-use shall be defined as easy to find addon, easy-to-learn by future staff in management accounting positions and easy-to-repair by maintainers. Also, Python is the most proficient language of the advisor, considering the human resource side. 
Therefore, Python is selected to be the language for prototyping. If later on, the MIS is deemed to be integrated into the overall ERP, this MIS can call the API of ERP to perform the tasks it is expected to perform. Also, some of the features of the current system can be migrated into the formal ERP system to increase the efficiency, and in that case the refactoring shall take the ERP into consideration with respect to the language selected. However, the remaining features, linked with the ERP with API, will not need refactoring with another language. Thus, it is likely that Python will be still used for the formal version of MIS. 

# Stage 4: prototyping

## Based on the meetings, there are seven key demand for the MIS. 

No| Management need   |Note | Data needed correspondently
--- | --- | ---|---
1 | Contract clearing |The invoicing for each contract, the payment on behave of the client, and the amount recovered | Invoice & Cash
2 | PM for contracts  | Profitability of each contract | Accrual Revenue & Standard cost
3 | PM for the company| Financial report adjusted for management use  |Accrual revenue & standard cost & Cash & other accrual transactions
4 | PM for sales    | The contribution and the recovery of debt for each sales person  | Cash & Accrual revenue & standard cost
5 | Customer value  | Profitability of each customer/Invoicing and collection of each customer |Invoice & Cash & Accrual revenue & Standard cost
6 | PM for equipment                    | Measure the success of procurement and maintenance of equipment | Standard cost & depreciation & Cash 
7 | Cash budgeting | | Cash

## The source of each data can be collected by 

Data | Source | Format | Note
--- | ---  | --- | ---
Cash | Bank statement | Electronic, from online banking | still need to be manually marked for the counter party or the type of expense
(above) | Cash statement | Manual record | 
Invoice | List of Invoice | Electronic, from the VAT system | The client name shall be identical to the client name in MIS 
Accrual revenue | Relocation record of equipment | Hardcopy confirmed by clients | Need to be processed either by the post-sales or by the admin
(above) | Daily price of equipment | Electronic records | extracted from contract by the admin
Standard cost | Relocation record of equipment | Hardcopy confirmed by clients | Need to be processed either by the post-sales or by the admin
(above) | Cost estimation | Electronic records | built by the CEO, can be changed but not frequently
Depreciation | Depreciation table | Based on the depreciation figures extracted from compliance accounting 

Thus, the key intermediate data are "accrual revenue" and "standard cost". 

## designing the modules

The list of module
1. (done) Equipment cost per contract measured by equipment*day (ED): with respect to the type of equipment. 
1. (done) Accrual revenue for each contract: Based on the contract information and ED
1. (done) Standard cost for each contract: Based on the cost estimation and ED 
1. (done) Contract clearing
1. (done) Contract PM
1. (done) Sales PM
1. (done) Customer value
1. Periodic financial reporting
1. Cash budget
1. Equipment PM (need PPE accounting numbers)

## standard cost rate
The advisor asked the CEO to provide estimation on the value of equipment, remaining life for the equipment, and normal rent price. Then use the normal rent price and remaining life to calculate the Internal Rate of Return ( IRR ) for a piece typical equipment. Then, using the IRR and annuity formula to calculate the daily expected revenue for each piece of equipment. 

## Implementing
The development is not difficult, however, many errors in the raw data was found. 

Reason of error | Solution
--- | --- 
A lack of training ( the preparer of the data ) | Provide additional training
Manual processing | Ask the preparer to retake
Missing data due to the original provider | Ask the preparer to find alternative data 

# Second meeting
The second meeting between the advisor and the financial manager is to provide the code which can already provide basic figures, as well as train the financial manager to use the system and get feedback.  

## Considering the method of implement: as binaries or as code

1. Binaries: easy to use but difficult to alter. No need for training. Especially, if the system was written in C++ the size of binaries would be small but since Python is used, the binary could be rather large and time consuming for re-compile for each change. 
1. Code: The usage of code would requires basic training such as "select the correct Anaconda prompt" and "type the correct path". However, since the user is only one person, the cost of training is not very high. 

## The method of data storage

Before the first test, the storage issues were intentionally omitted. Since the most important issue is to prove the concept of the MIS. However, the storage shall be considered once the usefulness is proven. 

There are two methods of data storage: Directly use the schedules input file as storage when the file become too big, then use another schedules file and let the code to read all schedule files., or, Load schedule into SQLite or other DBMS.  

Using schedules:

Benefits | Explanation
--- | ---
No extra efforts were needed| if the required input data changes, no further adaption on DBMS is needed.
The user can alter numbers on the schedules file easily | No need to develop database related functions in modify data  
Draw backs|Explanation
The version control of schedules could be difficult  | Could only rely on the users to control the files
The result cannot be accessed remotely | Can be overcome by sending the periodic report by email which is safer than setting up a server by the company.  


Using DBMS :
Benefits | Explanation
--- | ---
The integrity of data could be easily managed | But the key of MIS is to providing information instead of accurate storage. 
Version control is easy. | Need efforts on coding for version control 
Preventing the change of data  | Useful if more people were involved but currently there is only one user. 
Easy to be linked with PHP | Can be used to providing information and collecting information from the users when the system can accommodate more users. 

Therefore, the decision is to keep using schedules during the prototyping, but consider to switch to DBMS if the CEO need to access the data remotely. This information is provided to the financial manager during the second meeting so that an expectation of further changes could be formed. 

## New feature proposed: human resource related expenses
The accrual journal entries are, obviously, limited to the accrual revenues and costs, but also, the salaries and expenses. Thus, the salary and reimbursement can also be added to this system. During the meeting, the financial manager suggested to provide the journals about the salary and reimbursement but the advisor asked further about how the journals were made. As a result, the source of such journals, which is another form used by the human resource was talked about during the meeting and the manager agrees to provide an example for the form as a part of the schedule so that the system can incorporate such form to produce the final report as well as the human resource report. 
This change was supported by the proactive advisor and shows the possibility that the scope of such MIS shall reflect the need for the company instead of the pre-determined development contract. 