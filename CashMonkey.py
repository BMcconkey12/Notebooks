#!/usr/bin/env python
# coding: utf-8

# ## CashMonkey Copy
# 
# New notebook

# # Used to pull files from Cash Monkey's FTP site and transform for storage into a lakehouse.

# In[1]:


import pandas as pd
import datetime as dt
import ftplib


# In[2]:


HOSTNAME = "*****"
USERNAME = "*****"
PASSWORD = "*****"


# In[3]:


# get yesterday's date
date = dt.date.today() - dt.timedelta(days=1)
date = dt.datetime.strftime(date, "%Y-%m-%d")
#date = "2025-06-15" # manually set date


# In[4]:


def get_file(filename):
    """function to get files from CashMonkey"""

    # Connect FTP Server
    ftp_server = ftplib.FTP(HOSTNAME, USERNAME, PASSWORD)
 
    # force UTF-8 encoding
    ftp_server.encoding = "utf-8"

    try:
        with open(filename, "wb") as file:
            # Command for Downloading the file "RETR filename"
            ftp_server.retrbinary(f"RETR {filename}", file.write)

        file = open(filename, "r")
        df =  pd.read_csv(file, sep='\t')

        # remove file
        ftp_server.delete(filename)

        # close connection
        ftp_server.quit()

        return df
    except:
        pass


# In[5]:


# create filenames
order_file = "orders-" + date + ".txt"
inv_file = "inventory-" + date + ".txt"
lst_file = "listing-" + date + ".txt"
scans_file = "scans-" + date + ".txt"
ship_file = "shipments-" + date + ".txt"


# In[6]:


# call get_files() to pull files from CashMonkey to pandas dataframes
df_orders = get_file(order_file)
df_inv = get_file(inv_file)
df_lst = get_file(lst_file)
df_scans = get_file(scans_file)
df_ship = get_file(ship_file)


# In[7]:


# strip spaces from column names
df_orders.columns = df_orders.columns.str.replace(' ', '')
df_inv.columns = df_inv.columns.str.replace(' ', '')
df_lst.columns = df_lst.columns.str.replace(' ', '')
df_scans.columns = df_scans.columns.str.replace(' ', '')
df_ship.columns = df_ship.columns.str.replace(' ', '')


# In[8]:


# write returned data to lakehouse
if len(df_orders) > 0:
    spark.createDataFrame(df_orders.astype(str)).write.mode("append").option("mergeSchema", "true").saveAsTable("cm_orders_bronze")

if len(df_inv) > 0:
    spark.createDataFrame(df_inv.astype(str)).write.mode("append").option("mergeSchema", "true").saveAsTable("cm_inventory_bronze")

if len(df_lst) > 0:
    spark.createDataFrame(df_lst.astype(str)).write.mode("append").option("mergeSchema", "true").saveAsTable("cm_listings_bronze")

if len(df_scans) > 0:
    spark.createDataFrame(df_scans.astype(str)).write.mode("append").option("mergeSchema", "true").saveAsTable("cm_scans_bronze")

if len(df_ship) > 0:
    spark.createDataFrame(df_ship.astype(str)).write.mode("append").option("mergeSchema", "true").saveAsTable("cm_shipments_bronze")


# In[ ]:




