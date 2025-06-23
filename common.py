#!/usr/bin/env python
# coding: utf-8

# ## common
# 
# New notebook

# # Create a date table

# In[1]:


import datetime as dt
#from datetime import timedelta
#import numpy as np
import pandas as pd

df = pd.DataFrame()

start = dt.date(2015,1,1)
end = dt.date(dt.date.today().year + 2,12,31)

df["date"] = pd.date_range(start, end)
df["day"] = df["date"].dt.day
df["month"] = df["date"].dt.month
df["month_name"] = df["date"].dt.month_name()
df["weeknum"] = df["date"].dt.isocalendar().week
df["year"] = df["date"].dt.year
df["day_of_year"] = df["date"].dt.dayofyear
df["quarter"] = df["date"].dt.quarter
df["StartOfWeek"] = pd.to_datetime(df["date"]).dt.to_period("W-SAT").dt.start_time
df["weekday_name"] = df["date"].dt.day_name()
df["weeknum_year"] = df["weeknum"].astype(str) + "-" + df["year"].astype(str)

# calculate weekday_number from weekday_name
df.loc[df["weekday_name"] == "Sunday", "weekday_number"] = "1"
df.loc[df["weekday_name"] == "Monday", "weekday_number"] = "2"
df.loc[df["weekday_name"] == "Tuesday", "weekday_number"] = "3"
df.loc[df["weekday_name"] == "Wednesday", "weekday_number"] = "4"
df.loc[df["weekday_name"] == "Thursday", "weekday_number"] = "5"
df.loc[df["weekday_name"] == "Friday", "weekday_number"] = "6"
df.loc[df["weekday_name"] == "Saturday", "weekday_number"] = "7"

# loop through dataframe and check date equals today
today = dt.date.today()

for index, row in df.iterrows():
    if row["date"] == today:
        row["is_today"] = "yes"

# determine if dst
df['date_localized'] = df['date'].dt.tz_localize('America/New_York')

for index, row in df.iterrows():
    ddd = row["date_localized"]
    df.loc[index,"is_dst"] = ddd.dst() != pd.Timedelta(0)

df.drop(columns=["date_localized"], inplace=True)

# write dataframe to lakehouse
spark.createDataFrame(df).write.option("overwriteSchema", "true").mode("overwrite").saveAsTable("date_table")


# # Create an empty measures table

# In[2]:


# create a lakehouse table for calculated measures
d = {'empty': ["empty"]}
df = pd.DataFrame(data=d, dtype=object)

spark.createDataFrame(df).write.mode("overwrite").saveAsTable("calc_measures")

