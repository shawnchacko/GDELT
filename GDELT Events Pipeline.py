# Databricks notebook source
# MAGIC %pip install beautifulsoup4
# MAGIC %pip install langdetect
# MAGIC %pip install gdelt
# MAGIC %pip install geopandas

# COMMAND ----------

#Importing & declaration
from pyspark.sql.types import *
from pyspark.sql.functions import *
from pyspark.sql.types import *
import pandas as pd
import geopandas as gpd
from shapely.geometry import Polygon
import requests
import gdelt
import datetime
from datetime import datetime, timedelta


# COMMAND ----------

file_path = "/dbfs/tmp/"
file_name = "events_15min_bronze_"
current_time = datetime.now()
extraction_date = current_time.strftime("%Y %b %-d")

# Rounds to nearest 15min
def hour_rounder(t):
    return (t.replace(second=0, microsecond=0, minute=0, hour=t.hour)
               +timedelta(minutes=t.minute//15) * 15)
    
File_timestamp=hour_rounder(current_time).strftime("%Y%m%d%H%M%S")


# COMMAND ----------

# Extracting the 15 min event data from gdelt database
gdelt_15min = gdelt.gdelt(version=2)

events_15min = gdelt_15min.Search('2022 Dec 2',table='events',output='gpd')

# COMMAND ----------

# Geopandasdataframe(GPD) to Pandas dataframe
events_15min_bronze=pd.DataFrame(events_15min.drop(columns='geometry'))

#Adding the extraction time stamp to the file
events_15min_bronze['extraction_date'] = File_timestamp

# COMMAND ----------

#loading the data as parquet file into bronze folder 
events_15min_bronze.to_parquet(f'{file_path}bronze_data/bronze_events_15min/{file_name}{File_timestamp}')
