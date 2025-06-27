#!/usr/bin/env python
# coding: utf-8

# ## FRED Copy
# 
# New notebook

# In[1]:


# imports
from fredapi import Fred
import pandas as pd


# In[2]:


def get_code(code):
    """ Used to loop through a list of FRED codes returning data"""
    
    fred = Fred(api_key = 'xxxxx')
    fredData={}
    s = fred.get_series(code, observation_start='2010-01-01')
    fredData=s.reset_index()
    df = pd.DataFrame(fredData)
    df = df.fillna(0)
    df = df.rename(columns={'index':"Date",0:"Value"})

    return df


# In[3]:


codes = (
    'GDPPOT', 'GDPC1', 'DGS2', 'WGS10YR', 'UNRATE', 'AHETPI', 'OPHNFB',
    'INDPRO', 'FEDFUNDS', 'MORTGAGE30US', 'MRTSSM44X72USS', 'LNU05026645',
    'HSN1F', 'STLENI', 'ALTSALES', 'DGORDER', 'CPIAUCSL', 'CPILFESL',
    'FPCPITOTLZGUSA', 'STTMINWGNC', 'DCOILWTICO', 'POPTHM', 'NCURN',
    'NCPOP', 'NCMECK9POP', 'SP500', 'POILBREUSDM', 'DTWEXBGS',
    'RTWEXBGS', 'NCUR', 'GDP', 'UMCSENT', 'PSAVERT', 'CIVPART', 'PCEC',
    'PCE', 'PCECA', 'CPALTT01USM659N', 'MEHOINUSA672N', 'LNS11300060', 'DSPI',
    'PMSAVE', 'DSPIC96', 'A229RC0', 'A229RX0', 'LFWA64TTUSM647S', 'USREC',
    'CWUR0000SA0', 'RSAFS', 'RSAFSNA', 'MRTSSM448USS', 'LMJVTTUVUSM647S',
    'USTRADE', 'LNU00074597', 'LNU04074597', 'LNU02374593', 'LNU02374597',
    'MRTSSM452112EUSN','MRTSSM45330USN'
)

# loop through codes
for code in codes:
    df = get_code(code)
    spark.createDataFrame(df).write.mode("overwrite").saveAsTable(code) 

