#!/usr/bin/env python
# coding: utf-8

# ## NetSuite_Invoices
# 
# New notebook

# In[1]:


import pandas as pd
import json
import datetime
import requests
from shared_utils import passwords as pw
from oauthlib import oauth1
from requests_oauthlib import OAuth1Session


# In[2]:


def parse_suiteql_response(response):
    response_json = json.loads(response.text)

    items = response_json['items']
    offset = response_json['offset']
    count = response_json['count']
    total = response_json['totalResults']

    if response_json["hasMore"]:
        next_link = next(link for link in response_json["links"] if link["rel"] == "next")["href"]
    else:
        next_link = None

    return items, offset, count, total, next_link


# In[5]:


def run_suiteql_query(query):
    client = OAuth1Session(
        client_secret=pw.ns_consumer_secret,
        client_key=pw.ns_consumer_key,
        resource_owner_key=pw.ns_token_key,
        resource_owner_secret=pw.ns_token_secret,
        realm=pw.ns_account,
        signature_method=oauth1.SIGNATURE_HMAC_SHA256
    )

    url_account = pw.ns_account
    url = f"https://{url_account}.suitetalk.api.netsuite.com/services/rest/query/v1/suiteql"

    headers = {
        "Prefer": "transient",
        "Content-Type": "application/json"
    }

    body = json.dumps({"q": query})
    data = []

    while True:
        response = client.post(url=url, data=body, headers=headers)

        try: 
            response.raise_for_status()
        except requests.exceptions.HTTPError as e:
            raise Exception(f"SuiteQL request failed. {e}. Response Body: {response.text}")

        items, offset, count, total, next_link = parse_suiteql_response(response)

        #print(f"Retrieved {offset + count} of {total} results")

        data = data + items

        if next_link:
            url = next_link
        else:
            break

    return pd.json_normalize(data)


# In[14]:


# AP invoices
df_AP = run_suiteql_query("""
SELECT
	BUILTIN.DF( Transaction.Entity ) AS Customer,
	Transaction.Type AS TransactionType,
	Transaction.TranDate AS Date,
	Transaction.TranID AS DocumentNumber,	
	Transaction.OtherRefNum AS PONumber,
	Transaction.DueDate,
	TransactionAccountingLine.amount,
	TransactionAccountingLine.amountpaid,
	TransactionAccountingLine.amountunpaid
FROM
	Transaction
	INNER JOIN TransactionAccountingLine ON
		TransactionAccountingLine.Transaction = Transaction.ID
WHERE
	Transaction.Posting = 'T'
	AND Transaction.Voided = 'F'
	And Transaction.Type = 'VendBill'
ORDER BY
	Transaction.TranDate,
	Transaction.TranID""")


# In[15]:


# AR Invoices
df_AR = run_suiteql_query("""
SELECT
	BUILTIN.DF( Transaction.Entity ) AS Customer,
	Transaction.Type AS TransactionType,
	Transaction.TranDate AS Date,
	Transaction.TranID AS DocumentNumber,	
	Transaction.OtherRefNum AS PONumber,
	Transaction.DueDate,
	TransactionAccountingLine.amount,
	TransactionAccountingLine.amountpaid,
	TransactionAccountingLine.amountunpaid
FROM
	Transaction
	INNER JOIN TransactionAccountingLine ON
		TransactionAccountingLine.Transaction = Transaction.ID
WHERE
	Transaction.Posting = 'T'
	AND Transaction.Voided = 'F'
	And Transaction.Type = 'CustInvc'
ORDER BY
	Transaction.TranDate,
	Transaction.TranID""")


# In[18]:


# Write AR invoices to lakehouse
df_AR.drop("links", axis=1, inplace=True)
spark.createDataFrame(df_AR).write.mode("overwrite").saveAsTable("AR_Invoices_bronze")


# In[19]:


# Write AP invoices to lakehouse
df_AP.drop("links", axis=1, inplace=True)
spark.createDataFrame(df_AP).write.mode("overwrite").saveAsTable("AP_Invoices_bronze")


# In[ ]:




