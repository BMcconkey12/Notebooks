###  This script is used to take the DGR budget pieces from the NSPB budget 
#table for the specified year in a bronze lakehouse table.  They are then stored 
#as a monthly budget table where they can be used for the calculation of a 
#weighted daily DGR budget.

# imports
import pandas as pd

#load NSPB budget into pandas df
df = spark.sql("select * from budget_2025_bronze").toPandas()

# rename columns
df = df.rename(
    columns={
        "TP1":"1","TP2":"2","TP3":"3","TP4":"4","TP5":"5","TP6":"6",
        "TP7":"7","TP8":"8","TP9":"9","TP10":"10","TP11":"11","TP12":"12"
    }
)

# unpivot columns
df2 = pd.melt(
    df, 
    id_vars=["Year","Account","dept"], 
    value_vars=["1","2","3","4","5","6","7","8","9","10","11","12"]
)

# get store id for dept.  Add any new stores.
df2.loc[df2["dept"]=="156", "store"] = 11
df2.loc[df2["dept"]=="157", "store"] = 12
df2.loc[df2["dept"]=="158", "store"] = 13
df2.loc[df2["dept"]=="159", "store"] = 14
df2.loc[df2["dept"]=="160", "store"] = 15
df2.loc[df2["dept"]=="161", "store"] = 16
df2.loc[df2["dept"]=="162", "store"] = 17
df2.loc[df2["dept"]=="163", "store"] = 18
df2.loc[df2["dept"]=="164", "store"] = 19
df2.loc[df2["dept"]=="165", "store"] = 20
df2.loc[df2["dept"]=="166", "store"] = 21
df2.loc[df2["dept"]=="167", "store"] = 22
df2.loc[df2["dept"]=="168", "store"] = 23
df2.loc[df2["dept"]=="169", "store"] = 24
df2.loc[df2["dept"]=="170", "store"] = 25
df2.loc[df2["dept"]=="171", "store"] = 26
df2.loc[df2["dept"]=="172", "store"] = 27
df2.loc[df2["dept"]=="173", "store"] = 28
df2.loc[df2["dept"]=="174", "store"] = 29
df2.loc[df2["dept"]=="175", "store"] = 30
df2.loc[df2["dept"]=="176", "store"] = 31
df2.loc[df2["dept"]=="177", "store"] = 32
df2.loc[df2["dept"]=="178", "store"] = 33
df2.loc[df2["dept"]=="179", "store"] = 34
df2.loc[df2["dept"]=="180", "store"] = 35
df2.loc[df2["dept"]=="181", "store"] = 36
df2.loc[df2["dept"]=="182", "store"] = 37
df2.loc[df2["dept"]=="183", "store"] = 38
df2.loc[df2["dept"]=="184", "store"] = 39
df2.loc[df2["dept"]=="185", "store"] = 40
df2.loc[df2["dept"]=="186", "store"] = 41
df2.loc[df2["dept"]=="309", "store"] = 42
df2.loc[df2["dept"]=="311", "store"] = 44
df2.loc[df2["dept"]=="312", "store"] = 45
df2.loc[df2["dept"]=="313", "store"] = 46
df2.loc[df2["dept"]=="358", "store"] = 47
df2.loc[df2["dept"]=="152", "store"] = 50
df2.loc[df2["dept"]=="153", "store"] = 40
df2.loc[df2["dept"]=="154", "store"] = 23

# Donated Goods
df_DonatedGoods = df2.loc[df2["Account"].isin(["41102", "41152"])]
df_DonatedGoods = df_DonatedGoods.drop(columns=["Account"])
df_DonatedGoods["store"] = pd.to_numeric(df_DonatedGoods["store"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_DonatedGoods["variable"] = pd.to_numeric(df_DonatedGoods["variable"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_DonatedGoods["value"] = pd.to_numeric(df_DonatedGoods["value"], errors="coerce", downcast="float") # cast to numeric.  downcast can be float for float or integer for int.
df_DonatedGoods = df_DonatedGoods.groupby(["Year","store","variable"], as_index=False)["value"].sum()
df_DonatedGoods = df_DonatedGoods.rename(columns={"variable": "month", "value":"DonatedGoods"})
df_DonatedGoods = df_DonatedGoods.reset_index(drop=True)

# New Goods
df_NewGoods = df2.loc[df2["Account"].isin(["41105"])]
df_NewGoods = df_NewGoods.drop(columns=["Account"])
df_NewGoods["store"] = pd.to_numeric(df_NewGoods["store"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_NewGoods["variable"] = pd.to_numeric(df_NewGoods["variable"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_NewGoods["value"] = pd.to_numeric(df_NewGoods["value"], errors="coerce", downcast="float") # cast to numeric.  downcast can be float for float or integer for int.
df_NewGoods = df_NewGoods.groupby(["Year","store","variable"], as_index=False)["value"].sum()
df_NewGoods = df_NewGoods.rename(columns={"variable": "month", "value":"NewGoods"})
df_NewGoods = df_NewGoods.reset_index(drop=True)
df_NewGoods = df_NewGoods.reset_index(drop=True)
master_df = df_DonatedGoods.merge(df_NewGoods, how='left')

# Target
df_Target = df2.loc[df2["Account"].isin(["41106"])]
df_Target = df_Target.drop(columns=["Account"])
df_Target["store"] = pd.to_numeric(df_Target["store"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_Target["variable"] = pd.to_numeric(df_Target["variable"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_Target["value"] = pd.to_numeric(df_Target["value"], errors="coerce", downcast="float") # cast to numeric.  downcast can be float for float or integer for int.
df_Target = df_Target.groupby(["Year","store","variable"], as_index=False)["value"].sum()
df_Target = df_Target.rename(columns={"variable": "month", "value":"Target"})
df_Target = df_Target.reset_index(drop=True)
df_Target = df_Target.reset_index(drop=True)
master_df = master_df.merge(df_Target, how='left')

# ShopGoodwill
df_sgw = df2.loc[df2["Account"].isin(["41201", "41251"])]
df_sgw = df_sgw.drop(columns=["Account"])
df_sgw["store"] = pd.to_numeric(df_sgw["store"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_sgw["variable"] = pd.to_numeric(df_sgw["variable"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_sgw["value"] = pd.to_numeric(df_sgw["value"], errors="coerce", downcast="float") # cast to numeric.  downcast can be float for float or integer for int.
df_sgw = df_sgw.groupby(["Year","store","variable"], as_index=False)["value"].sum()
df_sgw = df_sgw.rename(columns={"variable": "month", "value":"ShopGoodwill"})
df_sgw = df_sgw.reset_index(drop=True)
master_df = master_df.merge(df_sgw, how='left')

# eBooks
df_eBooks = df2.loc[df2["Account"].isin(["41203", "41253"])]
df_eBooks = df_eBooks.drop(columns=["Account"])
df_eBooks["store"] = pd.to_numeric(df_eBooks["store"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_eBooks["variable"] = pd.to_numeric(df_eBooks["variable"], errors="coerce", downcast="integer") # cast to numeric.  downcast can be float for float or integer for int.
df_eBooks["value"] = pd.to_numeric(df_eBooks["value"], errors="coerce", downcast="float") # cast to numeric.  downcast can be float for float or integer for int.
df_eBooks = df_eBooks.groupby(["Year","store","variable"], as_index=False)["value"].sum()
df_eBooks = df_eBooks.rename(columns={"variable": "month", "value":"eBooks"})
df_eBooks = df_eBooks.reset_index(drop=True)
master_df = master_df.merge(df_eBooks, how='left')

# get donated goods percentages
category_df = spark.sql("select * from donated_categories_percentage_budget").toPandas()

# Create a holding df
donated_pct = pd.DataFrame()

# loop through category_df
for index, row in category_df.iterrows():    
    # get donated amt from master_df
    donatedamt = master_df.loc[master_df["store"]==row["Store"]]
    donatedamt["store"] = row["Store"]
    donatedamt = donatedamt[["store","month","DonatedGoods"]]
    donatedamt["textpct"] =  row["Textiles"]
    donatedamt["empct"] = row["EM"]
    donatedamt["warespct"] = row["Wares"]
    donatedamt["furnpct"] = row["Furniture"]
    donatedamt["shoespct"] = row["Shoes"]
    donatedamt["gridpct"] = row["Grid"]
    
    donated_pct = pd.concat([donated_pct, donatedamt])
    
donated_pct["Textiles"] = donated_pct["DonatedGoods"] * donated_pct["textpct"]
donated_pct["EM"] = donated_pct["DonatedGoods"] * donated_pct["empct"]
donated_pct["Wares"] = donated_pct["DonatedGoods"] * donated_pct["warespct"]
donated_pct["Furniture"] = donated_pct["DonatedGoods"] * donated_pct["furnpct"]
donated_pct["Shoes"] = donated_pct["DonatedGoods"] * donated_pct["shoespct"]
donated_pct["GRID"] = donated_pct["DonatedGoods"] * donated_pct["gridpct"]

donated_pct["store"] = pd.to_numeric(donated_pct["store"], downcast='integer')
donated_pct = donated_pct[["store","month","Textiles","EM","Wares","Furniture","Shoes","GRID"]]
master_df["store"] = pd.to_numeric(master_df["store"], downcast='integer')
master_df = master_df.merge(donated_pct, how='left')

spark.createDataFrame(master_df).write.mode("overwrite").saveAsTable("MonthlyBudget")

