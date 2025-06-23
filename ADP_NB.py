#!/usr/bin/env python
# coding: utf-8

# ## ADP_NB Copy
# 
# New notebook

# # This notebook is used to pull files from ADP and load the report df into lakehouse tables.

# In[1]:


# Imports
from shareplum import Site
from shareplum import Office365
from shareplum.site import Version
import paramiko
import pandas as pd
from email.message import EmailMessage
import smtplib
from shared_utils import passwords as pw


# In[2]:


def get_adp_file(filename):

    with paramiko.SSHClient() as ssh_client:
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname="sdgcl.adp.com", port=22, username=pw.adp_user, password=pw.adp_pass, banner_timeout=200)

        with ssh_client.open_sftp().open(f"./OUTBOUND/{filename}") as f:
            data = pd.read_csv(f)

    return data


# In[3]:


def remove_adp_file(filename):
    with paramiko.SSHClient() as ssh_client:
        ssh_client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        ssh_client.connect(hostname="sdgcl.adp.com", port=22, username=pw.adp_user, password=pw.adp_pass)
        ssh_client.open_sftp().remove(f"./OUTBOUND/{filename}")


# ## All Team Member

# In[4]:


atm_df = get_adp_file("AllTeamMember1.csv")

atm_df.rename(columns={
    'File Number':'File_Number', 
    'Position ID':'Position_ID', 
    'Birth Date':'Birth_Date',
    'Worker Category Description':'Worker_Category_Description', 
    'EEOC Job Classification':'EEOC_Job_Classification',
    'Home Department Description':'Home_Department_Description', 
    'Home Department Code':'Home_Department_Code',
    'Termination Date':'Termination_Date', 
    'HR Department':'HR_Department', 
    'Location Description':'Location_Description',
    'Hire Date':'Hire_Date', 
    'Hire/Rehire Date':'Rehire_Date', 
    'Pay Grade Description':'Pay_Grade_Description',
    'Primary Address: Full Address':'Primary_Address', 
    'Work Contact: Work Email':'Work_Email', 
    'District Area':'District_Area', 
    'Race Description':'Race_Description', 
    'FLSA Description':'FLSA_Description',
    'Previous Termination Date':'Previous_Termination_Date', 
    'Personal Contact: Personal Mobile':'Personal_Mobile',
    'Job Title Description':'Job_Title_Description', 
    'Regular Pay Rate Amount':'Regular_Pay_Rate', 
    'Annual Salary':'Annual_Salary',
    'Position Status':'Position_Status', 
    'Reports To Name':'Reports_To_Name', 
    'Reports to Email':'Reports_to_Email',
    'Position Effective Date':'Position_Effective_Date', 
    'Reports To Position ID':'Reports_To_Position_ID'
}, inplace=True)

spark.createDataFrame(atm_df.astype(str)).write.mode("overwrite").saveAsTable("all_team_members_bronze")

#remove_adp_file("AllTeamMember1.csv")


# ## Active Staff

# In[5]:


df = get_adp_file("ActiveStaff.csv")

df.rename(columns={
    'Position ID':'Position_ID', 
    'Payroll Name':'Payroll_Name', 
    'Job Title Description':'Job_Title_Description',
    'Home Department Description':'Home_Department_Description', 
    'Home Department Code':'Home_Department_Code',
    'Location Description':'Location_Description', 
    'Reports To Name':'Reports_To_Name', 
    'Position Effective Date':'Position_Effective_Date',
    'Hire Date':'Hire_Date', 
    'Hire/Rehire Date':'Rehire_Date', 
    'Termination Date':'Termination_Date',
    'Previous Termination Date':'Previous_Termination_Date', 
    'Position Status':'Position_Status',
    'Position Effective End Date':'Position_Effective_End_Date', 
    'Status Effective Date':'Status_Effective_Date',
    'Status Effective End Date':'Status_Effective_End_Date'
}, inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("active_staff_bronze")

#remove_adp_file("ActiveStaff.csv")


# ## Application Hires

# In[6]:


df = get_adp_file("ApplicationHires.csv")

df.rename(columns={
    'Last Name':'Last_Name',
    'First Name':'First_Name',
    'Hiring Manager':'Hiring_Manager',
    'Job Title':'Job_Title',
    'Requisition #':'Requisition_Number',
    'Application Date':'Application_Date',
    'Location Description':'Location_Description',
    'Hire/Rehire Date':'Rehire_Date',
    'Position ID':'Position_ID'
}, 
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("application_hires_bronze")

#remove_adp_file("ApplicationHires.csv")


# ## Application Source

# In[7]:


df = get_adp_file("ApplicationSource.csv")

df.rename(columns={
    'Last Name':'Last_Name',
    'First Name':'First_Name',
    'Hiring Manager':'Hiring_Manager',
    'Job Title':'Job_Title',
    'Requisition #':'Requisition_Number',
    'Application Date': 'Application_Date',
    'Location Description':'Location_Description'}, inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("application_source_bronze")

#remove_adp_file("ApplicationSource.csv")


# ## CNP Team Member

# In[8]:


df = get_adp_file("CNP_Team_Member.csv")

df.rename(columns={
    'File Number':'File_Number', 
    'Position ID':'Position_ID', 
    'Birth Date':'Birth_Date',
    'Worker Category Description':'Worker_Category_Description', 
    'EEOC Job Classification':'EEOC_Job_Classification',
    'Home Department Description':'Home_Department_Description', 
    'Termination Date':'Termination_Date',
    'HR Department':'HR_Department', 
    'Location Description':'Location_Description', 
    'Hire Date':'Hire_Date',
    'Hire/Rehire Date':'Rehire_Date', 
    'Pay Grade Description':'Pay_Grade_Description',
    'Primary Address: Full Address':'Full_Address', 
    'Work Contact: Work Email':'Work_Email', 
    'District Area':'District_Area', 
    'Race Description':'Race_Description', 
    'FLSA Description':'FLSA_Description',
    'Previous Termination Date':'Previous_Termination_Date', 
    'Personal Contact: Personal Mobile':'Personal_Mobile',
    'Job Title Description':'Job_Title_Description', 
    'Regular Pay Rate Amount':'Regular_Pay_Rate_Amount', 
    'Annual Salary':'Annual_Salary',
    'Position Status':'Position_Status', 
    'Reports To Name':'Reports_To_Name', 
    'Reports to Email':'Reports_to_Email',
    'Position Effective Date':'Position_Effective_Date', 
    'Reports To Position ID':'Reports_To_Position_ID'}, inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("cnp_team_member_bronze")

#remove_adp_file("CNP_Team_Member.csv")


# ## Home Department

# In[9]:


df = get_adp_file("HomeDepartment.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Home Department Description':'Home_Department_Description',
    'Position Effective Date':'Position_Effective_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("home_department_bronze")

#remove_adp_file("HomeDepartment.csv")


# ## Job Title Change

# In[10]:


df = get_adp_file("JobTitleChange.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Job Title Description':'Job_Title_Description',
    'Position Effective Date':'Position_Effective_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("job_title_change_bronze")

#remove_adp_file("JobTitleChange.csv")


# ## Labor Report without time clock field

# In[11]:


df = get_adp_file("Labor_No_TC.csv")

df = df.fillna(0)

# convert data frame columns to correct data types
df = df.astype({'Company Code': 'object'})
df = df.astype({'Worked In Department': 'float'})
df = df.astype({'Description': 'object'})
df = df.astype({'File Number (Pay Statements)': 'object'})
# df = df.astype({'Clock': 'object'})
df = df.astype({'Pay Date': 'datetime64[ns]'})
df['Gross Pay'] = df['Gross Pay'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Regular Earnings Total'] = df['Regular Earnings Total'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Overtime Earnings Total'] = df['Overtime Earnings Total'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Total Other Earnings'] = df['Total Other Earnings'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Regular Hours'] = df['Regular Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Overtime Hours'] = df['Overtime Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Coded Hours'] = df['Coded Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Total Hours'] = df['Total Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Labor Hours'] = df['Labor Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)

# rename columns
df.rename(columns={
    'Company Code':'Company_Code',
    'Worked In Department':'Worked_In_Department',
    'File Number (Pay Statements)':'File_Number',
    'Pay Date':'Pay_Date',
    'Gross Pay':'Gross_Pay',
    'Regular Hours':'Regular_Hours',
    'Overtime Hours':'Overtime_Hours',
    'Coded Hours':'Coded_Hours',
    'Total Hours':'Total_Hours',
    'Labor Hours':'Labor_Hours',
    'Job Title Description':'Job_Title_Description',
    'Regular Earnings Total':'Regular_Earnings_Total',
    'Overtime Earnings Total':'Overtime_Earnings_Total',
    'Total Other Earnings':'Total_Other_Earnings'},
inplace=True)

#remove records from labor_report_bronze that were returned from ADP
spark.sql("delete from labor_no_timeclock_bronze where year(Pay_Date) = YEAR(current_timestamp())")

spark.createDataFrame(df.astype(str)).write.mode("append").saveAsTable("labor_no_timeclock_bronze")

#remove_adp_file("Labor_No_TC.csv")


# ## Labor Report

# In[12]:


df = get_adp_file("NewLaborRpt.csv")

df = df.fillna(0)

# convert data frame columns to correct data types
df = df.astype({'Company Code': 'object'})
df = df.astype({'Worked In Department': 'float'})
df = df.astype({'Description': 'object'})
df = df.astype({'File Number (Pay Statements)': 'object'})
df = df.astype({'Clock': 'object'})
df = df.astype({'Pay Date': 'datetime64[ns]'})
df['Gross Pay'] = df['Gross Pay'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Regular Earnings Total'] = df['Regular Earnings Total'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Overtime Earnings Total'] = df['Overtime Earnings Total'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Total Other Earnings'] = df['Total Other Earnings'].replace(
    {r'\(': '-',r'\)': '',r'\$': '', ',': ''}, regex=True
).astype(float)
df['Regular Hours'] = df['Regular Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Overtime Hours'] = df['Overtime Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Coded Hours'] = df['Coded Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Total Hours'] = df['Total Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)
df['Labor Hours'] = df['Labor Hours'].replace(
    { ',': '',r'\(': '-', r'\)': ''}, regex=True
).astype(float)

df.drop(columns=["Job Title Description"], inplace = True)

# rename columns
df.rename(columns={
    'Company Code':'Company_Code',
    'Worked In Department':'Worked_In_Department',
    'File Number (Pay Statements)':'File_Number',
    'Pay Date':'Pay_Date',
    'Gross Pay':'Gross_Pay',
    'Regular Hours':'Regular_Hours',
    'Overtime Hours':'Overtime_Hours',
    'Coded Hours':'Coded_Hours',
    'Total Hours':'Total_Hours',
    'Labor Hours':'Labor_Hours',
    'Regular Earnings Total':'Regular_Earnings_Total',
    'Overtime Earnings Total':'Overtime_Earnings_Total',
    'Total Other Earnings':'Total_Other_Earnings'},
inplace=True)

# remove records from labor_report_bronze that were returned from ADP
spark.sql("delete from labor_report_bronze where year(Pay_Date) = YEAR(current_timestamp())")

spark.createDataFrame(df.astype(str)).write.option("overwriteSchema", "True").mode("append").saveAsTable("labor_report_bronze")

#remove_adp_file("NewLaborRpt.csv")


# ## Location Description

# In[13]:


df = get_adp_file("LocationDescription.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Location Description':'Location_Description',
    'Position Effective Date':'Position_Effective_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("location_description_bronze")

#remove_adp_file("LocationDescription.csv")


# ## Names

# In[14]:


df = get_adp_file("names.csv")

df.rename(columns={
    'File Number':'File_Number',
    'Position ID':'Position_ID',
    'First Name':'First_Name',
    'Last Name':'Last_Name',
    'Personal Contact: Personal Email':'Personal_Email',
    'Birth Date':'Birth_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("names_bronze")

#remove_adp_file("names.csv")


# ## New Hires

# In[15]:


authcookie = Office365(
    'https://gisp.sharepoint.com',
    username=pw.sp_user,
    password=pw.sp_pass
).GetCookies()

site = Site(
    'https://gisp.sharepoint.com/teams/NewHire',
    version=Version.v2016, authcookie=authcookie
)

sp_list = site.List('New Hire Checklist')
data = sp_list.GetListItems('GreaterThan2023_BM')

data_df = pd.DataFrame(data[0:])

data_df.rename(columns={
    'Location:HRAssignment':'HR_Assignment',
    'First Name':'First_Name',
    'Last Name':'Last_Name',
    'New Hire Paperwork':'New_Hire_Paperwork',
    'Offer Letter Sent':'Offer_Letter_Sent',
    'Pre-Employment Screening':'Pre_Employment_Screening',
    'Name Spelled Correctly Confirmation':'Name_Spelled_Correctly_Confirmation',
    'In Store Start':'In_Store_Start',
    'Nepotism-Checked':'Nepotism_Checked',
    'Basic Needs-Informed Of':'Basic_Needs_Informed_Of',
    'Offenses-Reviewed':'Offenses_Reviewed',
    'Approval Obtained':'Approval_Obtained',
    'MVR Discussed':'MVR_Discussed',
    'Rehire - Confirmed with HR':'Rehire_Confirmed_with_HR',
    'Created By':'Created_By',
    'Modified By':'Modified_By',
    'Preferred Name':'Preferred_Name',
    'Start Date':'Start_Date',
    'First Day Notes':'First_Day_Notes',
    'Job Title - Non-DGR':'Job_Title_Non_DGR',
    'Job Title - DGR':'Job_Title_DGR',
    'Youth Work Certificate':'Youth_Work_Certificate'},
    inplace=True)

data_df.drop(columns="New Hire Paperwork(Stopped Using 2024-07-26)", inplace=True)

spark.createDataFrame(data_df.astype(str)).write.mode("overwrite").saveAsTable("new_hires_bronze")


# ## 90 day reviews

# In[16]:


df = get_adp_file("NinetyDay.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    '90-Day Introductory Review':'Introductory_Review',
    'Review Due Date':'Review_Due_Date',
    'Review Completion Date':'Review_Completion_Date',
    'Review Progress':'Review_Progress'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("ninety_day_reviews_bronze")

#remove_adp_file("NinetyDay.csv")


# ## Timecards

# ## Pay Change

# In[17]:


df = get_adp_file("PayRate.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Regular Pay Rate Amount':'Regular_Pay_Rate_Amount',
    'Regular Pay Effective Date':'Regular_Pay_Effective_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("pay_rate_bronze")

#remove_adp_file("PayRate.csv")


# ## Promotions

# In[18]:


df = get_adp_file("Promotions.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Job Title Code':'Job_Title_Code',
    'Job Title Description':'Job_Title_Description',
    'Position Effective Date':'Position_Effective_Date',
    'Job Change Reason Description':'Job_Change_Reason_Description',
    'Regular Pay Rate Amount':'Regular_Pay_Rate_Amount',
    'Pay Grade Description':'Pay_Grade_Description',
    'Annual Salary':'Annual_Salary',
    'Position Status':'Position_Status',
    'Payroll Company Code':'Payroll_Company_Code'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("promotions_bronze")

#remove_adp_file("Promotions.csv")


# ## Reports To

# In[19]:


df = get_adp_file("ReportsTo.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Reports To Name':'Reports_To_Name',
    'Reports To Position ID':'Reports_To_Position_ID',
    'Reports To Effective Date':'Reports_To_Effective_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("reports_to_bronze")

#remove_adp_file("ReportsTo.csv")


# ## Requisition Status

# In[20]:


df = get_adp_file("Requisitions.csv")

df.rename(columns={
    'Requisition #':'Requisition_Number',
    'Job Title':'Job_Title',
    'Number of Positions':'Number_of_Positions',
    'Location Description':'Location_Description',
    'Hiring Manager':'Hiring_Manager',
    'Posting Status':'Posting_Status',
    'Number of Applications Per Requisition':'Number_of_Applications_Per_Requisition',
    'Posting Start Date':'Posting_Start_Date',
    'Posting End Date':'Posting_End_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("requisitions_bronze")

#remove_adp_file("Requisitions.csv")


# ## Retirement

# In[21]:


df = get_adp_file("Retirement.csv")

df.rename(columns={
    'DED CODE':'DED_CODE',
    'Position ID':'Position_ID',
    'File Number':'File_Number',
    'DEDUCTION AMT':'DEDUCTION_AMT',
    'DED CODE.1':'DED_CODE_1'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("retirement_bronze")

#remove_adp_file("Retirement.csv")


# ## Status Change

# In[22]:


df = get_adp_file("Status.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Position Status':'Position_Status',
    'Status Effective Date':'Status_Effective_Date'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("status_bronze")

#remove_adp_file("Status.csv")


# ## TA Metrics Report

# In[23]:


df = get_adp_file("TAMetrics.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'Legal First Name':'Legal_First_Name',
    'Legal Last Name':'Legal_Last_Name',
    'Job Title Description':'Job_Title_Description',
    'Position Start Date':'Position_Start_Date',
    'Location Description':'Location_Description',
    'Termination Date':'Termination_Date',
    'Position Status':'Position_Status'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("ta_metrics_bronze")

#remove_adp_file("TAMetrics.csv")


# ## Terminations

# In[24]:


df = get_adp_file("Terminations.csv")

df.rename(columns={
    'File Number':'File_Number',
    'Position ID':'Position_ID',
    'Termination Date':'Termination_Date',
    'Termination Reason Description':'Termination_Reason_Description',
    'Voluntary/Involuntary Termination Flag':'Voluntary_Involuntary_Termination_Flag'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("terminations_bronze")

#remove_adp_file("Terminations.csv")


# ## Timely Performance

# In[25]:


df = get_adp_file("Reviews.csv")

df.rename(columns={
    'Position ID':'Position_ID',
    'File Number':'File_Number',
    'Last Review Due Date':'Last_Review_Due_Date',
    'Last Review Completion Date':'Last_Review_Completion_Date',
    'Next Review Due Date':'Next_Review_Due_Date',
    'Last Review Rating':'Last_Review_Rating',
    'Last Review Overall Score':'Last_Review_Overall_Score',
    'Timely Status':'Timely_Status'},
inplace=True)

spark.createDataFrame(df.astype(str)).write.mode("overwrite").saveAsTable("reviews_bronze")

#remove_adp_file("Reviews.csv")


# In[ ]:




