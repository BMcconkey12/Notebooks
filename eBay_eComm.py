#!/usr/bin/env python
# coding: utf-8

# ## eBay_eComm
# 
# New notebook

# In[ ]:


get_ipython().system('pip install ebaysdk')


# In[ ]:


from ebaysdk.trading import Connection
from datetime import datetime
from datetime import timedelta
import pandas as pd


# In[ ]:


# Initialize the variable dictionary
requestVariables = {
    "ModTimeFrom": datetime.now() - timedelta(days = 5), 
    "ModTimeTo": datetime.now(),
    "Pagination": {
        "EntriesPerPage": "100",
        "PageNumber": "1"}
    }


# In[ ]:


# declare the configuration
api = Connection(
    config_file="/lakehouse/default/Files/ebay.yaml",
    debug=False,
    timeout = 120
)


# In[ ]:


# Function to call GetOrders and return a dictionary of response
def get_orders():
    response = api.execute("GetOrders", requestVariables)
    response_array = response.dict()
    return response_array


# In[ ]:


# Function to process the returned order array
def process_response(response_array):
    """ Used to populate an order array from the response data from 
    get_orders()
    """

    # How many pages are in this dataset
    numPages = int(response_array['PaginationResult']['TotalNumberOfPages'])

    # Get the OrderArray into a new dictionary
    order_array = response_array['OrderArray']['Order']

    # Define counter and loop through running GetOrders for each page
    i = 2

    while i < numPages + 1:
        requestVariables['Pagination']['PageNumber'] = i
        response_array = get_orders()
        order_array.extend(response_array['OrderArray']['Order'])
        i += 1

    return order_array


# In[ ]:


# Make the initial call to GetOrders
response_array = get_orders()
orderArray = process_response(response_array)


# In[ ]:


# Make orderArray dictionary a Pandas DataFrame
df = pd.DataFrame(orderArray)

# Get TransactionPrice column dict
df2 = df['TransactionArray'].apply(pd.Series)
df2 = df2['Transaction'].apply(pd.Series)

# How many transactions are in df2
df2Columns = len(df2.columns)
df2Columns = int(df2Columns)

# Create columns for totals
df2['TotalPrice'] = 0.00
df2['TotalShip'] = 0.00
df2['TotalHandle'] = 0.00

# Define a counter to loop through each column
columnCounter = 0

# Loop through each transaction column
while columnCounter < int(df2Columns):
    dfA = df2[columnCounter].apply(pd.Series)
    dfA1 = dfA['TransactionPrice'].apply(pd.Series)
    dfA1 = dfA1.fillna(0)
    dfA1A = dfA1['value'].astype(float)
    df2['TotalPrice'] += dfA1A
    if 'ActualShippingCost' in dfA.columns:
        dfB1 = dfA['ActualShippingCost'].apply(pd.Series)
        dfB1 = dfB1.fillna(0)
        dfB1A = dfB1['value'].astype(float)
        df2['TotalShip'] += dfB1A
    else:
        df2['TotalShip'] += 0
    if 'ActualHandlingCost' in dfA.columns:
        dfC1 = dfA['ActualHandlingCost'].apply(pd.Series)
        dfC1 = dfC1.fillna(0)
        dfC1A = dfC1['value'].astype(float)
        df2['TotalHandle'] += dfC1A
    else:
        df2['TotalHandle'] += 0
    df2 = df2.drop([columnCounter], axis = 1)
    columnCounter += 1


# In[ ]:


# Join original returned dataframe with totals dataframe
df = pd.concat([df, df2], axis=1)

# Replace any "NAN" value in the dataframe with 0
df = df.fillna(0)

# Replace zero dates
df = df.replace({'PaidTime': {0:'1900-01-01T00:00:00.001Z'}})
df = df.replace({'ShippedTime': {0:'1900-01-01T00:00:00.001Z'}})


# In[ ]:


# iterate through df and remove rows from existing table
for index, row in df.iterrows():
    orderid = row["OrderID"]
    spark.sql(f"delete from ebay_orders_bronze where OrderID = '{orderid}'")

# someday change this to delete where in a list of orderIDs to speed up.


# In[ ]:


dfsub = df[["OrderID","OrderStatus","CreatedTime","PaidTime","ShippedTime","TotalPrice","TotalShip","TotalHandle"]]


# In[ ]:


# append df to ebay_orders_bronze
spark.createDataFrame(dfsub).write.mode("append").option("mergeSchema", "true").saveAsTable("ebay_orders_bronze")

