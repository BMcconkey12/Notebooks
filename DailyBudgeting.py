#!/usr/bin/env python
# coding: utf-8

# ## DailyBudgeting
# 
# New notebook

# # This script takes a monthly department budget table and creates a weighted daily budget.

# In[1]:


import pandas as pd
import datetime as dt
import calendar


# ### Remove all data from tblDailyBudget for the new year.

# In[2]:


# The command is not a standard IPython magic command. It is designed for use within Fabric notebooks only.
# %%sql

# delete from tblDailyBudget where year(dtmDate) = 2025


# ### Declare the holiday dates for the upcoming year.

# In[3]:


# Holidays
holidays = {
    "Name": ["MLK","Easter","Thanksgiving","Christmas"], 
    "Date":["1/20/2025","4/20/2025","11/27/2025","12/25/2025"]
}


# ### Daily Sales and Donors, weighted.

# In[4]:


# get Daily Weights from table into a dataframe
weights_df = spark.sql("select * from buddailyweights where intYear = '2025'").toPandas()


# In[5]:


# get MonthlyBudget into dataframe
sales_df = spark.sql("Select * from monthlybudget").toPandas()
sales_df['Year'] = sales_df['Year'].astype('int32')
sales_df["store"] = sales_df["store"].astype('int32')


# In[6]:


# get donor budget into dataframe
donor_df = spark.sql("select * from donorbudget").toPandas()
donor_df = donor_df.rename(columns={"year":"Year","Month":"month"})
donor_df['Year'] = donor_df['Year'].astype('int32')
donor_df['store'] = donor_df['store'].astype('int32')
donor_df['month'] = donor_df['month'].astype('int8')


# In[7]:


# merge the sales with the donors
monthly_df = pd.merge(sales_df,donor_df,on=("Year", "store", "month"), how='outer')

# replace Nan values with 0
monthly_df = monthly_df.fillna(0)


# In[8]:


# create a blank dataframe to hold daily budget
columns = (
    "dtmDate","intStoreNumber","decTextileSales","decEMSales",
    "decWaresSales","decFurnitureSales","decShoeSales",
    "decNewGoodSales","decTargetSales","decEbook", "decECommerce",
    "decGridSales", "intDonors")
daily_budget_df = pd.DataFrame(columns=columns)


# In[9]:


# loop through each row of monthly_df
for index, row in monthly_df.iterrows():
    # set variables
    year = int(row["Year"])
    month = int(row["month"])
    day = 1
    store = int(row["store"])
    textile = float(row["Textiles"])
    em = float(row["EM"])
    wares = float(row["Wares"])
    new = float(row["NewGoods"])
    furniture = float(row["Furniture"])
    shoes = float(row["Shoes"])
    target = float(row["Target"])
    eBooks = float(row["eBooks"])
    sgw = float(row["ShopGoodwill"])
    GRID = float(row["GRID"])
    donors = float(row["Donors"])

    # calculated first and last day of month
    fdom = dt.datetime(year, month, day)
    next_month = fdom.replace(day=28) + dt.timedelta(days=4)
    ldom = next_month- dt.timedelta(days=next_month.day)

    # set daily weights from weights_df
    mon_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentMon"].sum())
    tue_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentTue"].sum())
    wed_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentWed"].sum())
    thu_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentThu"].sum())
    fri_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentFri"].sum())
    sat_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentSat"].sum())
    sun_weight = float(weights_df.loc[(weights_df["intYear"]==year) & (weights_df["intStoreNumber"]==store),"decPercentSun"].sum())

    # get number of days in the month by day of week
    mondaycount = 0
    tuesdaycount = 0
    wednesdaycount = 0
    thursdaycount = 0
    fridaycount = 0
    saturdaycount = 0
    sundaycount = 0

    yint = fdom.year
    mint = fdom.month
    stday = fdom.day
    enday = ldom.day 

    for dayint in range(stday, enday + 1):
        
        if dt.datetime(yint, mint,dayint).weekday() == 6:
            sundaycount += 1
        if dt.datetime(yint, mint,dayint).weekday() == 0:
            mondaycount += 1
        if dt.datetime(yint, mint,dayint).weekday() == 1:
            tuesdaycount += 1
        if dt.datetime(yint, mint,dayint).weekday() == 2:
            wednesdaycount += 1
        if dt.datetime(yint, mint,dayint).weekday() == 3:
            thursdaycount += 1
        if dt.datetime(yint, mint,dayint).weekday() == 4:
            fridaycount += 1
        if dt.datetime(yint, mint,dayint).weekday() == 5:
            saturdaycount += 1

    # Counts weighted by daily percentages
    mon_total = mondaycount * mon_weight
    tue_total = tuesdaycount * tue_weight
    wed_total = wednesdaycount * wed_weight
    thu_total = thursdaycount * thu_weight
    fri_total = fridaycount * fri_weight
    sat_total = saturdaycount * sat_weight
    sun_total = sundaycount * sun_weight

    # total weighted counts
    twc = mon_total + tue_total + wed_total + thu_total + fri_total + sat_total + sun_total

    if twc > 0: # should never be zero but...
        # Textile DOW Goals
        MonTextileGoal = (textile / twc) * mon_weight
        TueTextileGoal = (textile / twc) * tue_weight
        WedTextileGoal = (textile / twc) * wed_weight
        ThuTextileGoal = (textile / twc) * thu_weight
        FriTextileGoal = (textile / twc) * fri_weight
        SatTextileGoal = (textile / twc) * sat_weight
        SunTextileGoal = (textile / twc) * sun_weight
        
        # EM DOW Goals
        MonEMGoal = (em / twc) * mon_weight
        TueEMGoal = (em / twc) * tue_weight
        WedEMGoal = (em / twc) * wed_weight
        ThuEMGoal = (em / twc) * thu_weight
        FriEMGoal = (em / twc) * fri_weight
        SatEMGoal = (em / twc) * sat_weight
        SunEMGoal = (em / twc) * sun_weight
        
        # Wares DOW Goals
        MonWaresGoal = (wares / twc) * mon_weight
        TueWaresGoal = (wares / twc) * tue_weight
        WedWaresGoal = (wares / twc) * wed_weight
        ThuWaresGoal = (wares / twc) * thu_weight
        FriWaresGoal = (wares / twc) * fri_weight
        SatWaresGoal = (wares / twc) * sat_weight
        SunWaresGoal = (wares / twc) * sun_weight
        
        # New Goods DOW Goals
        MonNewGoodsGoal = (new / twc) * mon_weight
        TueNewGoodsGoal = (new / twc) * tue_weight
        WedNewGoodsGoal = (new / twc) * wed_weight
        ThuNewGoodsGoal = (new / twc) * thu_weight
        FriNewGoodsGoal = (new / twc) * fri_weight
        SatNewGoodsGoal = (new / twc) * sat_weight
        SunNewGoodsGoal = (new / twc) * sun_weight
        
        # Shoes DOW Goals
        MonShoesGoal = (shoes / twc) * mon_weight
        TueShoesGoal = (shoes / twc) * tue_weight
        WedShoesGoal = (shoes / twc) * wed_weight
        ThuShoesGoal = (shoes / twc) * thu_weight
        FriShoesGoal = (shoes / twc) * fri_weight
        SatShoesGoal = (shoes / twc) * sat_weight
        SunShoesGoal = (shoes / twc) * sun_weight
        
        # Furniture DOW Goals
        MonFurnitureGoal = (furniture / twc) * mon_weight
        TueFurnitureGoal = (furniture / twc) * tue_weight
        WedFurnitureGoal = (furniture / twc) * wed_weight
        ThuFurnitureGoal = (furniture / twc) * thu_weight
        FriFurnitureGoal = (furniture / twc) * fri_weight
        SatFurnitureGoal = (furniture / twc) * sat_weight
        SunFurnitureGoal = (furniture / twc) * sun_weight

        # Target DOW Goals
        MonTargetGoal = (target / twc) * mon_weight
        TueTargetGoal = (target / twc) * tue_weight
        WedTargetGoal = (target / twc) * wed_weight
        ThuTargetGoal = (target / twc) * thu_weight
        FriTargetGoal = (target / twc) * fri_weight
        SatTargetGoal = (target / twc) * sat_weight
        SunTargetGoal = (target / twc) * sun_weight
        
        # eBooks DOW Goals
        MonEBooksGoal = (eBooks / twc) * mon_weight
        TueEBooksGoal = (eBooks / twc) * tue_weight
        WedEBooksGoal = (eBooks / twc) * wed_weight
        ThuEBooksGoal = (eBooks / twc) * thu_weight
        FriEBooksGoal = (eBooks / twc) * fri_weight
        SatEBooksGoal = (eBooks / twc) * sat_weight
        SunEBooksGoal = (eBooks / twc) * sun_weight
        
        # SGW DOW Goals
        MonSGWGoal = (sgw / twc) * mon_weight
        TueSGWGoal = (sgw / twc) * tue_weight
        WedSGWGoal = (sgw / twc) * wed_weight
        ThuSGWGoal = (sgw / twc) * thu_weight
        FriSGWGoal = (sgw / twc) * fri_weight
        SatSGWGoal = (sgw / twc) * sat_weight
        SunSGWGoal = (sgw / twc) * sun_weight
        
        # GRID DOW Goals
        MonGRIDGoal = (GRID / twc) * mon_weight
        TueGRIDGoal = (GRID / twc) * tue_weight
        WedGRIDGoal = (GRID / twc) * wed_weight
        ThuGRIDGoal = (GRID / twc) * thu_weight
        FriGRIDGoal = (GRID / twc) * fri_weight
        SatGRIDGoal = (GRID / twc) * sat_weight
        SunGRIDGoal = (GRID / twc) * sun_weight
        
        # Donor DOW Goals
        MonDonorGoal = (donors / twc) * mon_weight
        TueDonorGoal = (donors / twc) * tue_weight
        WedDonorGoal = (donors / twc) * wed_weight
        ThuDonorGoal = (donors / twc) * thu_weight
        FriDonorGoal = (donors / twc) * fri_weight
        SatDonorGoal = (donors / twc) * sat_weight
        SunDonorGoal = (donors / twc) * sun_weight
    
    # get days in the month and counter
    dim = (ldom - fdom).days + 1
    counter = 1
    
    # interate through each day in the month
    while counter <= dim:
        loopdate = dt.datetime(yint, mint, counter)

        # create append dataframe
        append_df = pd.DataFrame(index=[0])

        if loopdate.weekday() == 0:
            append_df["dtmDate"] =  loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = MonTextileGoal
            append_df["decEMSales"] = MonEMGoal
            append_df["decWaresSales"] = MonWaresGoal
            append_df["decFurnitureSales"] = MonFurnitureGoal
            append_df["decShoeSales"] = MonShoesGoal
            append_df["decNewGoodSales"] = MonNewGoodsGoal
            append_df["decTargetSales"] = MonTargetGoal
            append_df["decEbook"] = MonEBooksGoal
            append_df["decECommerce"] = MonSGWGoal
            append_df["decGridSales"] = MonGRIDGoal
            append_df["intDonors"] = MonDonorGoal
            
        if loopdate.weekday() == 1:
            append_df["dtmDate"] = loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = TueTextileGoal
            append_df["decEMSales"] = TueEMGoal
            append_df["decWaresSales"] = TueWaresGoal
            append_df["decFurnitureSales"] = TueFurnitureGoal
            append_df["decShoeSales"] = TueShoesGoal
            append_df["decNewGoodSales"] = TueNewGoodsGoal
            append_df["decTargetSales"] = TueTargetGoal
            append_df["decEbook"] = TueEBooksGoal
            append_df["decECommerce"] = TueSGWGoal
            append_df["decGridSales"] = TueGRIDGoal
            append_df["intDonors"] = TueDonorGoal
            
        if loopdate.weekday() == 2:
            append_df["dtmDate"] = loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = WedTextileGoal
            append_df["decEMSales"] = WedEMGoal
            append_df["decWaresSales"] = WedWaresGoal
            append_df["decFurnitureSales"] = WedFurnitureGoal
            append_df["decShoeSales"] = WedShoesGoal
            append_df["decNewGoodSales"] = WedNewGoodsGoal
            append_df["decTargetSales"] = WedTargetGoal
            append_df["decEbook"] = WedEBooksGoal
            append_df["decECommerce"] = WedSGWGoal
            append_df["decGridSales"] = WedGRIDGoal
            append_df["intDonors"] = WedDonorGoal
        
        if loopdate.weekday() == 3:
            append_df["dtmDate"] = loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = ThuTextileGoal
            append_df["decEMSales"] = ThuEMGoal
            append_df["decWaresSales"] = ThuWaresGoal
            append_df["decFurnitureSales"] = ThuFurnitureGoal
            append_df["decShoeSales"] = ThuShoesGoal
            append_df["decNewGoodSales"] = ThuNewGoodsGoal
            append_df["decTargetSales"] = ThuTargetGoal
            append_df["decEbook"] = ThuEBooksGoal
            append_df["decECommerce"] = ThuSGWGoal
            append_df["decGridSales"] = ThuGRIDGoal
            append_df["intDonors"] = ThuDonorGoal
            
        if loopdate.weekday() == 4:
            append_df["dtmDate"] = loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = FriTextileGoal
            append_df["decEMSales"] = FriEMGoal
            append_df["decWaresSales"] = FriWaresGoal
            append_df["decFurnitureSales"] = FriFurnitureGoal
            append_df["decShoeSales"] = FriShoesGoal
            append_df["decNewGoodSales"] = FriNewGoodsGoal
            append_df["decTargetSales"] = FriTargetGoal
            append_df["decEbook"] = FriEBooksGoal
            append_df["decECommerce"] = FriSGWGoal
            append_df["decGridSales"] = FriGRIDGoal
            append_df["intDonors"] = FriDonorGoal

        if loopdate.weekday() == 5:
            append_df["dtmDate"] = loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = SatTextileGoal
            append_df["decEMSales"] = SatEMGoal
            append_df["decWaresSales"] = SatWaresGoal
            append_df["decFurnitureSales"] = SatFurnitureGoal
            append_df["decShoeSales"] = SatShoesGoal
            append_df["decNewGoodSales"] = SatNewGoodsGoal
            append_df["decTargetSales"] = SatTargetGoal
            append_df["decEbook"] = SatEBooksGoal
            append_df["decECommerce"] = SatSGWGoal
            append_df["decGridSales"] = SatGRIDGoal
            append_df["intDonors"] = SatDonorGoal
            
        if loopdate.weekday() == 6:
            append_df["dtmDate"] = loopdate
            append_df["intStoreNumber"] = store
            append_df["decTextileSales"] = SunTextileGoal
            append_df["decEMSales"] = SunEMGoal
            append_df["decWaresSales"] = SunWaresGoal
            append_df["decFurnitureSales"] = SunFurnitureGoal
            append_df["decShoeSales"] = SunShoesGoal
            append_df["decNewGoodSales"] = SunNewGoodsGoal
            append_df["decTargetSales"] = SunTargetGoal
            append_df["decEbook"] = SunEBooksGoal
            append_df["decECommerce"] = SunSGWGoal
            append_df["decGridSales"] = SunGRIDGoal
            append_df["intDonors"] = SunDonorGoal
            
        # append append_df to daily_budget_df
        daily_budget_df = pd.concat([daily_budget_df, append_df], ignore_index=True, sort=False)

        # increment counter
        counter += 1

# format the intDonors column
daily_budget_df["intDonors"] = daily_budget_df["intDonors"].astype(int)


# ### Production

# In[10]:


# get Production goals from lakehouse
prod_df = spark.sql("select * from production").toPandas()

# set daily_budget intPiecesHung from prod_df
daily_budget_df["intPiecesHung"] = 0

for index, row in daily_budget_df.iterrows():
    matching_rows = prod_df[prod_df["store"] == row["intStoreNumber"]]
    if not matching_rows.empty:
        daily_budget_df.at[index,"intPiecesHung"]= matching_rows["daily"]

# format the intPiecesHung column
daily_budget_df["intPiecesHung"] = daily_budget_df["intPiecesHung"].astype(int)


# ### Account for Holidays

# In[11]:


holidays_df = pd.DataFrame.from_dict(holidays)
holidays_df["dtmDate"] = pd.to_datetime(holidays_df["Date"]).dt.date


# In[12]:


# get a list of distinct store numbers from daily_budget_df
distinct_stores = daily_budget_df["intStoreNumber"].unique()


# In[13]:


# convert datetime / timestamp to date
daily_budget_df["dtmDate"] = daily_budget_df["dtmDate"].dt.date


# In[14]:


# for each date in holidays_df
for dtmDate in holidays_df["dtmDate"]:
    # get days in month
    daysInMonth = (calendar.monthrange(dtmDate.year, dtmDate.month)[1])
    
    # get values for each store
    for store_number in distinct_stores:
        text_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decTextileSales"].sum() / (daysInMonth - 1)
        em_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decEMSales"].sum() / (daysInMonth - 1)
        wares_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decWaresSales"].sum() / (daysInMonth - 1)
        furniture_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decFurnitureSales"].sum() / (daysInMonth - 1)
        shoes_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decShoeSales"].sum() / (daysInMonth - 1)
        newgoods_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decNewGoodSales"].sum() / (daysInMonth - 1)
        target_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decTargetSales"].sum() / (daysInMonth - 1)
        eBooks_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decEbook"].sum() / (daysInMonth - 1)
        eComm_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decECommerce"].sum() / (daysInMonth - 1)
        grid_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"decGridSales"].sum() / (daysInMonth - 1)
        donors_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"intDonors"].sum() / (daysInMonth - 1)
        hung_holiday = daily_budget_df.loc[(daily_budget_df['dtmDate']==dtmDate)&(daily_budget_df["intStoreNumber"]==store_number),"intPiecesHung"].sum() / (daysInMonth - 1)
        
        # set values in daily_budget_df to zero where store and dtmDate match  
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decTextileSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decEMSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decWaresSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decFurnitureSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decShoeSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decNewGoodSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decTargetSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decEbook"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decECommerce"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "decGridSales"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "intDonors"] = 0
        daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==dtmDate), "intPiecesHung"] = 0

        # loop through each day in month
        start_date = dt.date(dtmDate.year, dtmDate.month, 1)
        for day in range(daysInMonth):
            current_date = start_date + dt.timedelta(days=day)
            if current_date != dtmDate:
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decTextileSales"] = daily_budget_df["decTextileSales"]+text_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decEMSales"] = daily_budget_df["decEMSales"]+em_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decWaresSales"] = daily_budget_df["decWaresSales"]+wares_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decFurnitureSales"] = daily_budget_df["decFurnitureSales"]+furniture_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decShoeSales"] = daily_budget_df["decShoeSales"]+shoes_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decNewGoodSales"] = daily_budget_df["decNewGoodSales"]+newgoods_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decTargetSales"] = daily_budget_df["decTargetSales"]+target_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decEbook"] = daily_budget_df["decEbook"]+eBooks_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decECommerce"] = daily_budget_df["decECommerce"]+eComm_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "decGridSales"] = daily_budget_df["decGridSales"]+grid_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "intDonors"] = daily_budget_df["intDonors"]+donors_holiday
                daily_budget_df.loc[(daily_budget_df["intStoreNumber"] == store_number) & (daily_budget_df["dtmDate"]==current_date), "intPiecesHung"] = daily_budget_df["intPiecesHung"]+hung_holiday   


# In[15]:


# rename columns
daily_budget_df.rename(columns={"intPiecesHung": "intItems","decEbook": "decEBook"},inplace=True)

# add additional columns for legacy budgets
daily_budget_df["decComputerSales"] = 0
daily_budget_df["decFoodSales"] = 0
daily_budget_df["decDonations"] = 0
daily_budget_df["decEWaste"] = 0
daily_budget_df["decSalvage"] = 0
daily_budget_df["decDukeEnergy"] = 0
daily_budget_df["decCoupons"] = 0
daily_budget_df["decBistroPreparedFoodSales"] = 0
daily_budget_df["decBistroNonPreparedFoodSales"] = 0
daily_budget_df["decHousehold"] = 0


# In[16]:


# format all columns
daily_budget_df=daily_budget_df.replace("NULL",0)
daily_budget_df["dtmDate"] = pd.to_datetime(daily_budget_df["dtmDate"])
daily_budget_df["intStoreNumber"] = daily_budget_df["intStoreNumber"].astype(int)
daily_budget_df["decTextileSales"] = daily_budget_df["decTextileSales"].fillna(0).astype(float)
daily_budget_df["decEMSales"] = daily_budget_df["decEMSales"].fillna(0).astype(float)
daily_budget_df["decWaresSales"] = daily_budget_df["decWaresSales"].fillna(0).astype(float)
daily_budget_df["decFurnitureSales"] = daily_budget_df["decFurnitureSales"].fillna(0).astype(float)
daily_budget_df["decShoeSales"] = daily_budget_df["decShoeSales"].fillna(0).astype(float)
daily_budget_df["decNewGoodSales"] = daily_budget_df["decNewGoodSales"].fillna(0).astype(float)
daily_budget_df["decTargetSales"] = daily_budget_df["decTargetSales"].fillna(0).astype(float)
daily_budget_df["decComputerSales"] = daily_budget_df["decComputerSales"].fillna(0).astype(float)
daily_budget_df["decFoodSales"] = daily_budget_df["decFoodSales"].fillna(0).astype(float)
daily_budget_df["decDonations"] = daily_budget_df["decDonations"].fillna(0).astype(float)
daily_budget_df["decEBook"] = daily_budget_df["decEBook"].fillna(0).astype(float)
daily_budget_df["decECommerce"] = daily_budget_df["decECommerce"].fillna(0).astype(float)
daily_budget_df["decEWaste"] = daily_budget_df["decEWaste"].fillna(0).astype(float)
daily_budget_df["decSalvage"] = daily_budget_df["decSalvage"].fillna(0).astype(float)
daily_budget_df["decDukeEnergy"] = daily_budget_df["decDukeEnergy"].fillna(0).astype(float)
daily_budget_df["decCoupons"] = daily_budget_df["decCoupons"].fillna(0).astype(float)
daily_budget_df["decBistroPreparedFoodSales"] = daily_budget_df["decBistroPreparedFoodSales"].fillna(0).astype(float)
daily_budget_df["decBistroNonPreparedFoodSales"] = daily_budget_df["decBistroNonPreparedFoodSales"].fillna(0).astype(float)
daily_budget_df["decHousehold"] = daily_budget_df["decHousehold"].fillna(0).astype(float)
daily_budget_df["decGridSales"] = daily_budget_df["decGridSales"].fillna(0).astype(float)
daily_budget_df["intDonors"] = daily_budget_df["intDonors"].fillna(0).astype(int)
daily_budget_df["intItems"] = daily_budget_df["intItems"].fillna(0).astype(int)


# In[17]:


# write table to lakehouse
spark.createDataFrame(daily_budget_df).write.mode("append").saveAsTable("tblDailyBudget")

