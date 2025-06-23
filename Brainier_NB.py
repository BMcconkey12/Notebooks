#!/usr/bin/env python
# coding: utf-8

# ## Brainier_NB Copy
# 
# New notebook

# In[ ]:


import datetime as dt
import time
import re
import hashlib
import hmac
import base64
import requests
import pandas as pd
import logging
#from custom_lib import passwords as pw #GISP Environment
from shared_utils import passwords as pw #Goodwill Environment


# In[ ]:


def create_auth_header(username, digest):
    """ This function creates the authorization header. """

    return 'hmac %s:[%s] %s'%(username, digest, pw.br_coid)


# In[ ]:


def create_hmac_digest(base_message):
    """ This function creates the secure digest string. """

    message = bytes(base_message, 'utf-8')
    secret = bytes(pw.br_cust_key, 'utf-8')
    signature = bytes(
        hmac.new(
            secret,
            message,
            digestmod=hashlib.sha256
        ).digest().hex().upper(),
        'utf-8')
    signature_b64 = base64.b64encode(signature)
    return signature_b64.decode('utf-8')


# In[ ]:


def create_authstring(method, path, date, username):
    """ This function creates the authstring used in the create_hmac_digest
    function.
    """

    formatted_date = re.sub("\\-| ", "", date)
    formatted_path = path.split('?',1)[0]
    return '%s+%s+%s+%s'%(method, formatted_path, formatted_date, username)


# In[ ]:


def make_request(method, path, username):
    """ This function is the one that is called by the referencing code.  It
    uses the create_authstring function and the create_hmac_digest function
    to then make a request from the base url and supplied path and return a
    pandas dataframe of the results.
    """

    request_date = dt.datetime.utcnow().strftime("%Y-%m-%d %H:%M:%S")
    auth_string = create_authstring(method, path, request_date, username)
    digest = create_hmac_digest(auth_string)
    auth_header = create_auth_header(username,digest)

    headers = {
        'Authentication': auth_header,
        'Elan-Date': request_date,
        'API-Client-IP': '96.10.47.136'
    }

    resp = requests.request(method, headers = headers, url = "https://goodwillsp.brainier.com"+path)

    results = resp.json()
    
    return results


# In[ ]:


# Create a log file
log_path = "/lakehouse/default/Files/"
file_name = "log_" + dt.date.today().strftime("%Y%m%d") + ".txt"
logging.basicConfig(filename=log_path + file_name, force = True, filemode='w',level=logging.INFO)##or DEBUG
logging.info("The script started at " + dt.datetime.now().strftime("%H:%M:%S") + "\n")


# In[ ]:


# Get users
results = make_request("GET", "/rest/brainier/user", "biuser")
users_df = pd.DataFrame.from_dict(results['DATA'])

users_df.fillna(users_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# rename aux_ID-Salesforce and aux_Position Start Date
users_df.rename(columns={
    "aux_ID-Salesforce":"aux_ID_Salesforce",
    "aux_Position Start Date":"aux_Position_Start_Date",
    "aux_Hire Date":"aux_Hire_Date",
    "aux_Rehire Date":"aux_Rehire_Date"
}, inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(users_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("users_bronze")

    execution_time = time.time() - start_time
    logging.info("Users inserted in " + str(execution_time) + "\n")
    

except Exception as e:
    logging.error("Users" + ": " + str(e) + "\n")


# In[ ]:


# Get groups
results = make_request("GET", "/rest/brainier/group", "biuser")
groups_df = pd.DataFrame.from_dict(results['DATA'])

groups_df['CREATION_DATE']= pd.to_datetime(groups_df['CREATION_DATE'])

groups_df.fillna(groups_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(groups_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("groups_bronze")

    execution_time = time.time() - start_time
    logging.info("Groups inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Groups" + ": " + str(e) + "\n")


# In[ ]:


# Get learning objects
results = make_request("GET", "/rest/brainier/object", "biuser")
objects_df = pd.DataFrame.from_dict(results['DATA'])

objects_df['OBJECT_EXP_DATE'] = pd.to_datetime(objects_df['OBJECT_EXP_DATE'])
objects_df['OBJECT_REL_DATE'] = pd.to_datetime(objects_df['OBJECT_REL_DATE'])

objects_df.fillna(objects_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# rename columns
objects_df.rename(columns={
    "aux_Difficulty Level":"aux_Difficulty_Level",
    "aux_Intensity Level":"aux_Intensity_Level",
    "aux_SF Program ID":"aux_SF_Program_ID",
    "aux_Continuing Education Unit (CEU)":"aux_Continuing_Education_Unit"
}, inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(objects_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("objects_bronze")

    execution_time = time.time() - start_time
    logging.info("Objects inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Objects" + ": " + str(e) + "\n")


# In[ ]:


# Get user object records
results = make_request("GET", "/rest/brainier/record", "biuser")
records_df = pd.DataFrame.from_dict(results['DATA'])

# Insert records from the users_df into the Users SQL table
records_df['COMPLETED_TS']= pd.to_datetime(records_df['COMPLETED_TS'])
records_df['START_TS']= pd.to_datetime(records_df['START_TS'])

records_df.fillna(records_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(records_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("records_bronze")

    execution_time = time.time() - start_time
    logging.info("Records inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Records" + ": " + str(e) + "\n")


# In[ ]:


# Get assignments
results = make_request("GET", "/rest/brainier/batches", "biuser")
assignments_df = pd.DataFrame.from_dict(results['DATA'])

# Insert records from the users_df into the Users SQL table
assignments_df['BATCH_CREATION_DATE']= pd.to_datetime(assignments_df['BATCH_CREATION_DATE'])
assignments_df['DUE_DATE']= pd.to_datetime(assignments_df['DUE_DATE'])

assignments_df.fillna(assignments_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(assignments_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("assignments_bronze")

    execution_time = time.time() - start_time
    logging.info("Assignments inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Assignments" + ": " + str(e) + "\n")


# In[ ]:


# Get tags
results = make_request("GET", "/rest/brainier/tags", "biuser")
tags_df = pd.DataFrame.from_dict(results['DATA'])

tags_df.fillna(assignments_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(tags_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("tags_bronze")

    execution_time = time.time() - start_time
    logging.info("Tags inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Tags" + ": " + str(e) + "\n")


# In[ ]:


# Get authors
results = make_request("GET", "/rest/brainier/author", "biuser")
authors_df = pd.DataFrame.from_dict(results['DATA'])

authors_df.fillna(authors_df.dtypes.replace({
    'float64': 0.0,
    'object': 'NULL',
    'int64': 0,
    'datetime64[ns]': '1900-01-01 00:00:00'
}), inplace=True)

# insert dataframe as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(authors_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("authors_bronze")

    execution_time = time.time() - start_time
    logging.info("Groups inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Groups" + ": " + str(e) + "\n")


# In[ ]:


# Get class schedules
class_schedules_df = pd.DataFrame()

for i in objects_df["OBJECT_ID"]:
    try:
        results = make_request("GET",f"/rest/brainier/schedule/{i}", "biuser")
        if results.get("DATA"):
            schedules_df = pd.DataFrame.from_dict(results['DATA'])
            class_schedules_df = pd.concat([class_schedules_df, schedules_df])
    except:
        pass

class_schedules_df.fillna(class_schedules_df.dtypes.replace({
        'float64': 0.0,
        'object': 'NULL',
        'int64': 0,
        'datetime64[ns]': '1900-01-01 00:00:00'
    }), inplace=True)

class_schedules_df.rename(columns={
    "aux_Program Engagement-Stage":"aux_Program_Engagement_Stage",
    "aux_SF Program ID":"aux_SF_Program_ID",
    "aux_Mode of Delivery":"aux_Mode_of_Delivery"
}, inplace=True)

# insert sf_records_df as a SQL table
start_time = time.time()

try:
    spark.createDataFrame(class_schedules_df.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("schedules_bronze")

    execution_time = time.time() - start_time
    logging.info("Schedules inserted in " + str(execution_time) + "\n")

except Exception as e:
    logging.error("Schedules" + ": " + str(e) + "\n")

