#!/usr/bin/env python
# coding: utf-8

# ## EventTemple_NB Copy
# 
# New notebook

# # This script is used to connect to EventTemple and retrieve data to be used in our analytics environment.
# 

# In[ ]:


import requests
import json
from flatten_json import flatten
import pandas as pd
import numpy as np
from time import sleep
from time import strftime
from time import gmtime


# ## This returns a token to be used to authenticate the data requests.

# In[ ]:


def get_token():
    url = " https://api.eventtemple.com/oauth/token"
    client_id = "*****"
    secret = "*****"
    header = {
        "accept": "application/json"
    }
    body = {
        "grant_type": "client_credentials",
        "client_id": client_id,
        "client_secret": secret,
        "scope": "crm_read"
    }

    response = requests.post(url, headers=header, data=body)
    data_dict = json.loads(response.text)
    token = data_dict["access_token"]
    token = "Bearer " + token

    return token


# ## This returns event details.

# In[ ]:


def get_events(token):
    
    #token = get_token()
    loop_value = 1
    page = 1
    df = pd.DataFrame()

    while loop_value == 1:
        url = f"https://api.eventtemple.com/v2/events?page[size]=100&page[number]={page}"
        headers = {
            "accept": "application/json",
            "Authorization": token
        }

        response = requests.get(url, headers=headers)

        try:
            response_json = json.loads(response.text)

            response_data = response_json["data"]
            response_flat = (flatten(record, '.') for record in response_data)

            response_df = pd.DataFrame(response_flat)

            df = pd.concat([df,response_df], ignore_index=True)

            page += 1

        except:
            loop_value = 0
    
    return df


# ## This returns booking details.

# In[ ]:


def get_bookings(token):
    
    #token = get_token()
    
    loop_value = 1
    page = 1

    df = pd.DataFrame()

    while loop_value == 1:
        url = f"https://api.eventtemple.com/v2/bookings?page[size]=100&page[number]={page}"
        headers = {
            "accept": "application/json",
            "Authorization": token
        }

        response = requests.get(url, headers=headers)

        try:
            response_json = json.loads(response.text)

            response_data = response_json["data"]
            response_flat = (flatten(record, '.') for record in response_data)

            response_df = pd.DataFrame(response_flat)

            df = pd.concat([df,response_df], ignore_index=True)

            page += 1

        except:
            loop_value = 0

    return df


# ## This returns space details.

# In[ ]:


def get_spaces(token):
    
    #token = get_token()
    
    loop_value = 1
    page = 1

    df = pd.DataFrame()

    while loop_value == 1:
        url = f"https://api.eventtemple.com/v2/spaces?page[size]=100&page[number]={page}"
        headers = {
            "accept": "application/json",
            "Authorization": token
        }

        response = requests.get(url, headers=headers)

        try:
            response_json = json.loads(response.text)

            response_data = response_json["data"]
            response_flat = (flatten(record, '.') for record in response_data)

            response_df = pd.DataFrame(response_flat)

            df = pd.concat([df,response_df], ignore_index=True)

            page += 1

        except:
            loop_value = 0

    return df


# # This returns room setups

# In[ ]:


def get_setups(token):
    
    #token = get_token()
    loop_value = 1
    page = 1

    df = pd.DataFrame()

    while loop_value == 1:
        url = f"https://api.eventtemple.com/v2/setup_styles?page[size]=100&page[number]={page}"
        headers = {
            "accept": "application/json",
            "Authorization": token
        }

        response = requests.get(url, headers=headers)

        try:
            response_json = json.loads(response.text)

            response_data = response_json["data"]
            response_flat = (flatten(record, '.') for record in response_data)

            response_df = pd.DataFrame(response_flat)

            df = pd.concat([df,response_df], ignore_index=True)

            page += 1

        except:
            loop_value = 0

    return df


# ## Get token and populate dataframes.

# In[ ]:


token = get_token()
sleep(60)


# In[ ]:


spaces = get_spaces(token)
sleep(60)


# In[ ]:


events = get_events(token)
sleep(60)


# In[ ]:


bookings = get_bookings(token)
sleep(60)


# In[ ]:


setups = get_setups(token)


# In[ ]:


# convert time columns from float to int.  Not sure why I'm doing this step...
events["attributes.start_time"] = events["attributes.start_time"].fillna(0).astype(np.int64)
events["attributes.end_time"] = events["attributes.end_time"].fillna(0).astype(np.int64)
bookings["attributes.start_time"] = bookings["attributes.start_time"].fillna(0).astype(np.int64)
bookings["attributes.end_time"] = bookings["attributes.end_time"].fillna(0).astype(np.int64)


# In[ ]:


# convert time columns from int to datatime.  This seems to work without calling the preceding cell.
events["attributes.start_time"] = pd.to_datetime(events["attributes.start_time"], unit="s")
events["attributes.end_time"] = pd.to_datetime(events["attributes.end_time"], unit="s")
bookings["attributes.start_time"] = pd.to_datetime(bookings["attributes.start_time"], unit="s")
bookings["attributes.end_time"] = pd.to_datetime(bookings["attributes.end_time"], unit="s")


# ## Write dataframes to lakehouse tables.

# In[ ]:


spark.createDataFrame(spaces.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("spaces_bronze")
spark.createDataFrame(events.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("events_bronze")
spark.createDataFrame(bookings.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("bookings_bronze")
spark.createDataFrame(setups.astype(str)).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("setups_bronze")

