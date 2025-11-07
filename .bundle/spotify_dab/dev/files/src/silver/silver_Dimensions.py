# Databricks notebook source
# MAGIC %load_ext autoreload
# MAGIC %autoreload 2

# COMMAND ----------

from pyspark.sql.functions import * 
from pyspark.sql.types import * 

import os
import sys

project_path = os.path.join(os.getcwd(),'..','..')
sys.path.append(project_path)
#print(os.path.join(os.getcwd(),'..','..'))

from utils.transformation import reusable

# COMMAND ----------

# MAGIC %md
# MAGIC ### Autoloader for DimUser table

# COMMAND ----------

df_user = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimUser/checkpoint")\
            .load("abfss://bronze@spotifystoragekiran.dfs.core.windows.net/DimUser")


## Add .option("schemaEvolutionMode","addNewColumns") -- to add new column if the tables is added with new column in the future.

df_user.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations on Users Table

# COMMAND ----------

df_user = df_user.withColumn("user_name",upper(col("user_name")))\
    .withColumn("country",upper(col("country")))
df_user.display()

# COMMAND ----------

df_user_obj = reusable()
#df_user.display()

df_user = df_user_obj.dropColumns(df_user,['_rescued_data'])
#df_user.display()
df_user = df_user.dropDuplicates(['user_id'])

df_user.display()


# COMMAND ----------

df_user.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimUser/checkpoint")\
            .trigger(once=True)\
                .start("abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimUser/data")

# COMMAND ----------

# MAGIC %md
# MAGIC ### DimArtist table data

# COMMAND ----------

df_artist = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimArtist/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@spotifystoragekiran.dfs.core.windows.net/DimArtist")

df_artist.display()

# COMMAND ----------

df_art_obj = reusable()

df_artist = df_art_obj.dropColumns(df_artist,['_rescued_data'])

df_artist = df_artist.dropDuplicates(['artist_id'])
df_artist.display()

# COMMAND ----------

df_artist.writeStream.format("delta")\
    .outputMode("append")\
        .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimArtist/checkpoint")\
            .trigger(once=True)\
                .start("abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimArtist/data")

            

# COMMAND ----------

# MAGIC %md
# MAGIC ### writing the above tables (DimUser and Dim artist) in the catalog in silver schema 

# COMMAND ----------

# This is for the DimArtist table

df_artist.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimArtist/checkpoint")\
              .trigger(once=True)\
              .option("path","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimArtist/data")\
              .toTable("spotify_cata.silver.DimArtist")
         
                


# COMMAND ----------

##This is for the DimUser table 

df_user.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimUser/checkpoint")\
              .trigger(once=True)\
              .option("path","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimUser/data")\
              .toTable("spotify_cata.silver.DimUser")

# COMMAND ----------

# MAGIC %md
# MAGIC ## ** DimTrack **

# COMMAND ----------

df_track = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimTrack/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@spotifystoragekiran.dfs.core.windows.net/DimTrack")

df_track.display()

# COMMAND ----------

df_track = df_track.withColumn("durationFlag",when(col("duration_sec")< 150,"low")\
    .when(col("duration_sec") < 300 , "medium")\
        .otherwise("high"))

df_track = df_track.withColumn("track_name",regexp_replace(col("track_name"),"-"," "))

df_track.display()

# COMMAND ----------

df_track = reusable().dropColumns(df_track,['_rescued_data'])

df_track.display()

# COMMAND ----------

##This is for the DimTrack table 

df_track.writeStream.format("delta")\
              .outputMode("append")\
              .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimTrack/checkpoint")\
              .trigger(once=True)\
              .option("path","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimTrack/data")\
              .toTable("spotify_cata.silver.DimTrack")

# COMMAND ----------

# MAGIC %md
# MAGIC
# MAGIC ### ** DimDate **

# COMMAND ----------

df_date = spark.readStream.format("cloudFiles")\
    .option("cloudFiles.format","parquet")\
        .option("cloudFiles.schemaLocation","abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimDate/checkpoint")\
            .option("schemaEvolutionMode","addNewColumns")\
                .load("abfss://bronze@spotifystoragekiran.dfs.core.windows.net/DimDate")

df_date.display()

# COMMAND ----------

df_date = reusable().dropColumns(df_date, ['_rescued_data'])

df_date.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimDate/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/DimDate/data")\
    .toTable("spotify_cata.silver.DimDate")

# COMMAND ----------

# MAGIC %md
# MAGIC ### FactStream Table Ingetsion
# MAGIC
# MAGIC

# COMMAND ----------

df_factstream = spark.readStream.format("cloudFiles")\
  .option("cloudFiles.format","parquet")\
    .option("cloudFiles.schemaLocation","abfss://silver@spotifystoragekiran.dfs.core.windows.net/FactStream/checkpoint")\
      .option("cloudFiles.schemaEvolutionMode","addNewColumns")\
        .load("abfss://bronze@spotifystoragekiran.dfs.core.windows.net/FactStream")

df_factstream.display()

# COMMAND ----------

df_factstream = reusable().dropColumns(df_factstream,['_rescued_data'])

df_factstream.display()

# COMMAND ----------

df_factstream.writeStream.format("delta")\
    .outputMode("append")\
    .option("checkpointLocation", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/FactStream/checkpoint")\
    .trigger(once=True)\
    .option("path", "abfss://silver@spotifystoragekiran.dfs.core.windows.net/FactStream/data")\
    .toTable("spotify_cata.silver.FactStream")