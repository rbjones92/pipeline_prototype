# Robert Jones
# Create dataframe for green_taxi_data
# 8/18/22

from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import re


spark = SparkSession.builder.getOrCreate()

# MISSING...DEFINE SCHEMAS #

class MakeDfs():


    green_docs = 'C:/Users/Bob\Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/New_York_Taxi_Data/Green Taxi*'
    yellow_docs = 'C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/New_York_Taxi_Data/Yellow Taxi*'
    hv_docs = 'C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/New_York_Taxi_Data/High Volume*'
    fh_docs = 'C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/New_York_Taxi_Data/For-Hire*'

    ### GREEN ###

    # Read all green
    df_green = spark.read.option('header',True).csv(green_docs)

    # Keep only alphanumeric characters
    df_green = df_green.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_green.columns])

    # Write to single dataframe
    df_green.coalesce(1).write.parquet('C:/Users/Bob/Desktop/SpringBoard/Python_Project/NYC_Taxi_Capstone/Pipeline_Prototype/DFs/df_green.parquet')


    ### YELLOW ###

    # Read all yellow
    df_yellow = spark.read.option('header',True).csv(yellow_docs)

    # Keep only alphanumeric characters
    df_yellow = df_yellow.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_yellow.columns])

    # Write to single dataframe
    df_yellow.coalesce(1).write.parquet('C:/Users/Bob/Desktop/SpringBoard/Python_Project/NYC_Taxi_Capstone/Pipeline_Prototype/DFs/df_yellow.parquet')


    ### HIGH VOLUME ###

    # Read all high volume
    df_hv  = spark.read.option('header',True).csv(hv_docs)

    # Keep only alphanumeric characters
    df_hv = df_hv.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_hv.columns])

    # Write to single dataframe
    df_hv.coalesce(1).write.parquet('C:/Users/Bob/Desktop/SpringBoard/Python_Project/NYC_Taxi_Capstone/Pipeline_Prototype/DFs/df_hv.parquet')


    ### FOR HIRE ###

    # Read all For-Hire docs
    df_fh = spark.read.option('header',True).csv(fh_docs)

    # Keep only alphanumeric characters
    df_fh = df_fh.select([F.col(col).alias(re.sub("[^0-9a-zA-Z$]+","",col)) for col in df_fh.columns])

    # Write to single dataframe
    df_hv.coalesce(1).write.parquet('C:/Users/Bob/Desktop/SpringBoard/Python_Project/NYC_Taxi_Capstone/Pipeline_Prototype/DFs/df_fh.parquet')



