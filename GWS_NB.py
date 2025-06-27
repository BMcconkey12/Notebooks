#!/usr/bin/env python
# coding: utf-8

# ## GWS_NB Copy
# 
# New notebook

# # Script to run to get Avionte Bold data

# In[1]:


import requests
import pandas as pd
import json
import numpy as np


# ## Get Token
# 

# In[2]:


def get_token():
  """ function to get an authroization token from Avionte."""

  import requests

  url = "https://api.avionte.com/authorize/token"

  payload = 'grant_type=client_credentials&client_id=xxxxx&client_secret=xxxxx&scope=avionte.aero.compasintegrationservice'
  headers = {
    'x-api-key': 'xxxxx',
    'Content-Type': 'application/x-www-form-urlencoded'
  }

  response = requests.request("POST", url, headers=headers, data=payload)

  results = response.json()

  token = results["access_token"]

  token = "Bearer " + token

  return token


# ## Branches

# In[3]:


token = get_token()

url = "https://api.avionte.com/front-office/v1/branch"

payload = {}
headers = {
  "accept": "application/json",
  'x-api-key': 'xxxxx',
  'Authorization': token
}

response = requests.get(url, headers=headers)

branches = response.json()

branches = pd.json_normalize(branches)

spark.createDataFrame(branches.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("branches_bronze")


# ## Companies

# In[4]:


def get_company_ids():
    """ function to return all company ids from Avionte."""

    loop_value = 1
    page = 0
    pageSize = 500

    master_list = []
    while loop_value == 1:
        url = f"https://api.avionte.com/front-office/v1/companies/ids/{page}/{pageSize}"

        payload = {}
        headers = {
          'Tenant': 'GWS',
          'x-api-key': 'xxxxx',
          'Authorization': token
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        company_ids = response.json()

        master_list = master_list + company_ids

        page += 1

        if len(company_ids) == 0:
            loop_value = 0

    all_ids = pd.DataFrame(master_list)
    all_ids.rename(columns={0: "id"}, inplace=True)

    return all_ids


# In[5]:


def get_company_info(id):
    """ function to return all company info for provided id from Avionte."""
    
    url = f"https://api.avionte.com/front-office/v1/company/{id}"

    payload = {}
    headers = {"Tenant": "GWS","x-api-key": "xxxxx","Authorization": token}

    response = requests.request("GET", url, headers=headers, data=payload)

    company = response.json()

    company_df = pd.json_normalize(company)

    # Remove columns that are lists
    list_cols = (company_df.applymap(type) == list).all()
    list_cols = list_cols.index[list_cols].tolist()
    company_df.drop(list_cols, axis=1, inplace=True)
    company_df.rename(columns={"name": "companyname"}, inplace=True)

    company_df["createdDate"] = company_df["createdDate"].str.replace("0001-01-01T00:00:00Z","")

    company_df["createdDate"] = pd.to_datetime(company_df["createdDate"], format="mixed", dayfirst=False, errors="coerce")
    company_df["lastUpdatedDate"] = pd.to_datetime(company_df["lastUpdatedDate"], format="mixed", dayfirst=False, errors="coerce")
    company_df["latestActivityDate"] = pd.to_datetime(company_df["latestActivityDate"], format="mixed", dayfirst=False, errors="coerce")

    # write df to lakehouse table
    spark.createDataFrame(company_df.astype(str)).write.option("mergeSchema", "true").mode("append").saveAsTable("companies_bronze")


# In[6]:


# Get token
token = get_token()

# Get a list of all talent IDs from Avionte
company_ids = get_company_ids()

# Get a list of existing talent IDs from SQL
existing_ids = spark.read.table("companies_bronze").toPandas()

# merging and dropping common rows
existing_ids["id"] = existing_ids["id"].astype(np.int64)
company_ids["id"] = company_ids["id"].astype(np.int64)
company_ids_sub = pd.merge(company_ids, existing_ids, on=['id'], how='outer', indicator=True).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)

# loop through talent ids 
for index,row in company_ids_sub.iterrows():
    id = row["id"]
    company = get_company_info(id)


# ## Contacts

# In[7]:


def get_contact_ids():
    """ function to return all company ids from Avionte."""

    loop_value = 1
    page = 0
    pageSize = 500

    master_list = []
    while loop_value == 1:
        url = f"https://api.avionte.com/front-office/v1/contacts/ids/{page}/{pageSize}"

        payload = {}
        headers = {
            'Tenant': 'GWS',
            'x-api-key': 'xxxxx',
            'Authorization': token
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        contact_ids = response.json()

        master_list = master_list + contact_ids

        page += 1

        if len(contact_ids) == 0:
            loop_value = 0

    all_ids = pd.DataFrame(master_list)
    all_ids.rename(columns={0: "id"}, inplace=True)

    return all_ids


# In[8]:


def get_contact_info(id):
    """ function to return all contact info, for provided id, from Avionte."""
    
    url = f"https://api.avionte.com/front-office/v1/contact/{id}"

    payload = {}
    
    headers = {
        'Tenant': 'GWS',
        'x-api-key': 'xxxxx',
        'Authorization': token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    contact = response.json()

    contact_df = pd.json_normalize(contact)

    # Remove columns that are lists
    list_cols = (contact_df.applymap(type) == list).all()
    list_cols = list_cols.index[list_cols].tolist()
    contact_df.drop(list_cols, axis=1, inplace=True)

    contact_df["createdDate"] = pd.to_datetime(contact_df["createdDate"], format="mixed", dayfirst=False, errors="coerce")
    contact_df["lastUpdatedDate"] = pd.to_datetime(contact_df["lastUpdatedDate"], format="mixed", dayfirst=False, errors="coerce")
    contact_df["latestActivityDate"] = pd.to_datetime(contact_df["latestActivityDate"], format="mixed", dayfirst=False, errors="coerce")

    # write to lakehouse table
    spark.createDataFrame(contact_df.astype(str)).write.option("mergeSchema", "true").mode("append").saveAsTable("contacts_bronze")


# In[9]:


# Get token
token = get_token()

# Get a list of all contact IDs from Avionte
contact_ids = get_contact_ids()

# Get a list of existing contact ids
existing_ids = spark.read.table("contacts_bronze").toPandas()

# merging and dropping common rows
existing_ids["id"] = existing_ids["id"].astype(np.int64)
contact_ids["id"] = contact_ids["id"].astype(np.int64)
contact_ids_sub = pd.merge(contact_ids, existing_ids, on=['id'], how='outer', indicator=True).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)

for index,row in contact_ids_sub.iterrows():
    id = row["id"]
    contact = get_contact_info(id)


# ## Jobs

# In[10]:


def get_job_ids():
    """ Add a while loop that increments the page number until lenth of job_ids == 0"""
    loop_value = 1
    page = 0
    pageSize = 500

    master_list = []
    while loop_value == 1:
        url = f"https://api.avionte.com/front-office/v1/jobs/ids/{page}/{pageSize}"

        payload = {}
        headers = {
          'Tenant': 'GWS',
          'x-api-key': 'xxxxx',
          'Authorization': token
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        job_ids = response.json()

        master_list = master_list + job_ids

        page += 1

        if len(job_ids) == 0:
            loop_value = 0

    all_ids = pd.DataFrame(master_list)
    all_ids.rename(columns={0: "id"}, inplace=True)

    return all_ids


# In[11]:


def get_job_info(id):
    url = f"https://api.avionte.com/front-office/v1/job/{id}"

    payload = {}
    headers = {
      'Tenant': 'GWS',
      'x-api-key': 'xxxxx',
      'Authorization': token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    talent = response.json()

    job_df = pd.json_normalize(talent)

    # Remove columns that are lists
    list_cols = (job_df.applymap(type) == list).all()
    list_cols = list_cols.index[list_cols].tolist()
    job_df.drop(list_cols, axis=1, inplace=True)

    job_df["startDate"] = pd.to_datetime(job_df["startDate"], format="mixed", dayfirst=False, errors="coerce")
    job_df["endDate"] = pd.to_datetime(job_df["endDate"], format="mixed", dayfirst=False, errors="coerce")
    job_df["createdDate"] = pd.to_datetime(job_df["createdDate"], format="mixed", dayfirst=False, errors="coerce")
    job_df["lastUpdatedDate"] = pd.to_datetime(job_df["lastUpdatedDate"], format="mixed", dayfirst=False, errors="coerce")
    job_df["latestActivityDate"] = pd.to_datetime(job_df["latestActivityDate"], format="mixed", dayfirst=False, errors="coerce")

    return job_df


# In[12]:


# Get token
token = get_token()

job_info = pd.DataFrame()

# Get a list of all talent IDs from Avionte
job_ids = get_job_ids()

# Get a list of existing talent IDs from SQL
existing_ids = spark.read.table("jobs_bronze").toPandas()

# merging and dropping common rows
Eexisting_ids["id"] = existing_ids["id"].astype(np.int64)
job_ids["id"] = job_ids["id"].astype(np.int64)
job_ids = pd.merge(job_ids, existing_ids, on=['id'], how='outer', indicator=True).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)

for index,row in job_ids.iterrows():
    id = row["id"]
    job_df = get_job_info(id)
    job_df = pd.DataFrame.from_dict(job_df)
    job_info = pd.concat([job_info, job_df])

# remove duplicate column
job_info.drop(columns=["worksiteAddressId"], inplace=True)

# write to lakehouse table
spark.createDataFrame(job_info.astype(str)).write.mode("append").option("mergeSchema", "true").saveAsTable("jobs_bronze")


# ## Placements

# In[15]:


def get_placement_ids():
    """ function to return all placement ids from Avionte."""

    loop_value = 1
    page = 0
    pageSize = 500

    master_list = []
    while loop_value == 1:
        url = f"https://api.avionte.com/front-office/v1/placements/ids/{page}/{pageSize}"

        payload = {}
        headers = {
          'Tenant': 'GWS',
          'x-api-key': 'xxxxx',
          'Authorization': token
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        placement_ids = response.json()

        master_list = master_list + placement_ids

        page += 1

        if len(placement_ids) == 0:
            loop_value = 0

    all_ids = pd.DataFrame(master_list)
    all_ids.rename(columns={0: "id"}, inplace=True)

    return all_ids


# In[16]:


def get_placement_info(id):
    """ function to retrieve placement info, for provided id, from Avionte."""
    
    url = f"https://api.avionte.com/front-office/v1/placement/{id}"

    payload = {}
    headers = {
      'Tenant': 'GWS',
      'x-api-key': 'xxxxx',
      'Authorization': token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    placement = response.json()

    placement_df = pd.json_normalize(placement)
    
    # Remove columns that are lists
    list_cols = (placement_df.applymap(type) == list).all()
    list_cols = list_cols.index[list_cols].tolist()
    placement_df.drop(list_cols, axis=1, inplace=True)

    # write to lakehouse table
    spark.createDataFrame(placement_df.astype(str)).write.option("mergeSchema", "true").mode("append").saveAsTable("placement_bronze")


# In[17]:


# Get token
token = get_token()

# Get a list of all talent IDs from Avionte
placement_ids = get_placement_ids()

# Get a list of existing talent IDs from SQL
existing_ids = spark.read.table("placement_bronze").toPandas()

# merging and dropping common rows
existing_ids["id"] = existing_ids["id"].astype(np.int64)
placement_ids["id"] = placement_ids["id"].astype(np.int64)
placement_ids_sub = pd.merge(placement_ids, existing_ids, on=['id'], how='outer', indicator=True).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)

# loop through placement ids 
for index,row in placement_ids_sub.iterrows():

    # get placement info for placement id
    id = row["id"]
    placement = get_placement_info(id)


# ## Talent

# In[18]:


def get_talent_ids():
    """ function to return all talent ids from Avionte."""

    loop_value = 1
    page = 0
    pageSize = 500

    master_list = []
    while loop_value == 1:
        url = f"https://api.avionte.com/front-office/v1/talents/ids/{page}/{pageSize}"

        payload = {}
        headers = {
          'Tenant': 'GWS',
          'x-api-key': 'xxxxxx',
          'Authorization': token
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        talent_ids = response.json()

        master_list = master_list + talent_ids

        page += 1

        if len(talent_ids) == 0:
            loop_value = 0

    all_ids = pd.DataFrame(master_list)
    all_ids.rename(columns={0: "id"}, inplace=True)

    return all_ids


# In[19]:


def get_talent_info(id):
    """ function to return talent info for provided id from Avionte."""

    url = f"https://api.avionte.com/front-office/v1/talent/{id}"

    payload = {}
    headers = {
      'Tenant': 'GWS',
      'x-api-key': 'xxxxx',
      'Authorization': token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    talent = response.json()

    talent_df = pd.json_normalize(talent, max_level=1)
        
    # remove addresses column
    talent_df.drop(["addresses"], axis=1, inplace=True)
    
    # convert date columns.  I do this because the dates from Avionte come in various formats and Pandas is smart enough to figure them out.
    talent_df["birthday"] = pd.to_datetime(talent_df["birthday"], format="mixed", dayfirst=False, errors="coerce")
    talent_df["hireDate"] = pd.to_datetime(talent_df["hireDate"], format="mixed", dayfirst=False, errors="coerce")
    talent_df["latestActivityDate"] = pd.to_datetime(talent_df["latestActivityDate"], format="mixed", dayfirst=False, errors="coerce")
    talent_df["createdDate"] = pd.to_datetime(talent_df["createdDate"], format="mixed", dayfirst=False, errors="coerce")
    talent_df["lastContacted"] = pd.to_datetime(talent_df["lastContacted"], format="mixed", dayfirst=False, errors="coerce")

    # write to lakehouse table
    spark.createDataFrame(talent_df.astype(str)).write.option("mergeSchema", "true").mode("append").saveAsTable("talent_bronze")# option("overwriteSchema", "true")


# In[20]:


# Get token
token = get_token()

# Get a list of all talent IDs from Avionte
talent_ids = get_talent_ids()

# Get a list of existing talent IDs from SQL
existing_ids = spark.read.table("talent_bronze").toPandas()

# merging and dropping common rows
existing_ids["id"] = existing_ids["id"].astype(np.int64)
talent_ids["id"] = talent_ids["id"].astype(np.int64)
talent_ids_sub = pd.merge(talent_ids, existing_ids, on=['id'], how='outer', indicator=True).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)

# Get Talent info for IDs that remain in talent_ids_sub. 
for index,row in talent_ids_sub.iterrows():
    id = row["id"]
    talent = get_talent_info(id)


# ## Users

# In[22]:


def get_user_ids():
    """ function to return all user ids from Avionte."""

    loop_value = 1
    page = 0
    pageSize = 500

    master_list = []
    while loop_value == 1:
        url = f"https://api.avionte.com/front-office/v1/users/ids/{page}/{pageSize}"

        payload = {}
        headers = {
          'Tenant': 'GWS',
          'x-api-key': 'xxxxx',
          'Authorization': token
        }

        response = requests.request("GET", url, headers=headers, data=payload)

        user_ids = response.json()

        master_list = master_list + user_ids

        page += 1

        if len(user_ids) == 0:
            loop_value = 0

    all_ids = pd.DataFrame(master_list)
    all_ids.rename(columns={0: "id"}, inplace=True)

    return all_ids


# In[23]:


def get_user_info(id):
    """ function to get user info, for provided id, from Avionte."""

    url = f"https://api.avionte.com/front-office/v1/user/{id}"

    payload = {}
    headers = {
      'Tenant': 'GWS',
      'x-api-key': 'xxxxx',
      'Authorization': token
    }

    response = requests.request("GET", url, headers=headers, data=payload)

    try:
      users = json.loads(response.text)
      user = pd.json_normalize(users)
      list_cols = (user.applymap(type) == list).all()
      list_cols = list_cols.index[list_cols].tolist()
      user.drop(list_cols, axis=1, inplace=True)

      user["createdDate"] = pd.to_datetime(user["createdDate"], format="mixed", dayfirst=False, errors="coerce")
      user["lastUpdatedDate"] = pd.to_datetime(user["lastUpdatedDate"], format="mixed", dayfirst=False, errors="coerce")


      # write to lakehouse table
      spark.createDataFrame(user.astype(str)).write.option("mergeSchema", "true").mode("append").saveAsTable("users_bronze")
    except:
      pass


# In[24]:


# Get token
token = get_token()

# Get a list of all user IDs from Avionte
user_ids = get_user_ids()

# Get a list of existing user IDs from SQL
existing_ids = spark.read.table("users_bronze").toPandas()

# merging and dropping common rows
existing_ids["id"].fillna(0, inplace=True)
existing_ids["id"] = existing_ids["id"].astype(np.int64)
user_ids["id"].fillna(0, inplace=True)
user_ids["id"] = user_ids["id"].astype(np.int64)
user_ids_sub = pd.merge(user_ids, existing_ids, on=['id'], how='outer', indicator=True).query("_merge != 'both'").drop('_merge', axis=1).reset_index(drop=True)

for index,row in user_ids_sub.iterrows():
    id = row["id"]
    get_user_info(id)        


# ## Delta table optimization

# In[25]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# optimize branches_bronze;
# optimize companies_bronze;
# optimize contacts_bronze;
# optimize jobs_bronze;
# optimize placement_bronze;
# optimize talent_bronze;
# optimize users_bronze;


# In[26]:


# get rows from the table
df = spark.sql("select * from users_bronze")

# drop duplicate rows based on column identifier
df1 = df.dropDuplicates(["id"])

# overwrite existing lakehouse table
df1.write.format("delta").mode("overwrite").save("Tables/users_bronze")

