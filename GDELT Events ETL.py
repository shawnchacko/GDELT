# Databricks notebook source
# MAGIC %pip install beautifulsoup4
# MAGIC %pip install langdetect
# MAGIC %pip install gdelt
# MAGIC %pip install geopandas

# COMMAND ----------

# MAGIC %sh 
# MAGIC mkdir -p /dbfs/tmp/bronze_data
# MAGIC mkdir -p /dbfs/tmp/silver_data
# MAGIC mkdir -p /dbfs/tmp/gold_data

# COMMAND ----------

# MAGIC %sql 
# MAGIC 
# MAGIC CREATE DATABASE IF NOT EXISTS gdelt_events location '/tmp/';

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

# COMMAND ----------

file_path = "/dbfs/tmp/"
file_name = "events_bronze_"
writer_date = datetime.datetime.now().strftime('%Y-%m-%d')
writer_timestamp = datetime.datetime.now().strftime('%Y-%m-%d %X')

# COMMAND ----------

# MAGIC %md
# MAGIC #####Extracting RAW file from GDELT Events Database & Loading as parquet file into bronze folder

# COMMAND ----------

gd = gdelt.gdelt(version=2)

events = gd.Search(date=['2022 22 Sep','2022 23 Sep'],table='events',coverage=True,output='gpd',normcols=True)
print("=>Succeeded")

# COMMAND ----------

# Geodataframe to Pandas dataframe
events_bronze=pd.DataFrame(events.drop(columns='geometry'))

#Adding the extraction time stamp to the file
events_bronze['extraction_date'] = writer_timestamp
#events_latlong_geolocations = pd.DataFrame(events_bronze.drop(columns='geometry'))
#df_final = gpd.GeoDataFrame(df2, geometry=gpd.points_from_xy(df2.actiongeolong, df2.actiongeolat))

# COMMAND ----------

# loading the bronze parquet file into DBFS
events_bronze.to_parquet(f'{file_path}bronze_data/{file_name}{writer_date}')

# COMMAND ----------

# MAGIC %sql
# MAGIC --Setting up the gdelt database
# MAGIC use gdelt_events;

# COMMAND ----------

# Creating spark datframe and the bronze table
events_bronze_sparkDF=spark.createDataFrame(events_bronze)
events_bronze_sparkDF.write.saveAsTable("events_bronze")

# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Part1: Preparing the Sanctions data as a parquet file and as a silver table and loading into Silver folder & Silver database

# COMMAND ----------

#selecting only the required columns
pd_sanctions_events_temp=events[['actor1code', 'actor1name', 'actor1countrycode','eventcode', 'cameocodedescription', 'eventbasecode', 'eventrootcode'
       ,'quadclass', 'goldsteinscale','actor2code','actor2name', 'actor2countrycode','actiongeolat', 'actiongeolong']]

# converting geodataframe to pandas dataframe
pd_sanctions_events = pd.DataFrame(pd_sanctions_events_temp)

# adding extraction timestamp to the silver data
pd_sanctions_events['extraction_date'] = writer_timestamp

# filtering the column with required filter
sanctions_events_silver=pd_sanctions_events[pd_sanctions_events['cameocodedescription'].str.contains("economic sanctions")]

# COMMAND ----------

# This section converts pandas datafrma eto geopandas dataframe
#sanctions_events_silver_geo = gpd.GeoDataFrame(pd_sanctions_events_filter, geometry=gpd.points_from_xy(pd_sanctions_events_filter.actiongeolong, pd_sanctions_events_filter.actiongeolat))

# COMMAND ----------

# MAGIC %sql
# MAGIC use gdelt_events;

# COMMAND ----------

sanctions_events_silver_sparkDF=spark.createDataFrame(sanctions_events_silver)
sanctions_events_silver_sparkDF.write.saveAsTable("sanctions_events_silver")


# COMMAND ----------

# MAGIC %md 
# MAGIC ##### Part2: Preparing the Social Unrest data as a parquet file and as a silver table and loading into Silver folder & Silver database

# COMMAND ----------

pd_social_unrest_events_silver=events_bronze[['sqldate','monthyear', 'eventrootcode', 'goldsteinscale', 'nummentions','avgtone', 'actiongeocountrycode', 'actiongeolat', 'actiongeolong','extraction_date']]

# COMMAND ----------

social_unrest_events_silver_sparkDF=spark.createDataFrame(pd_social_unrest_events_silver)


# COMMAND ----------

social_unrest_events_silver_sparkDF.write.mode('append').saveAsTable("social_unrest_events_silver")
