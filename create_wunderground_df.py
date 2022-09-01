# Robert Jones
# 7.18.22
# Create one dataframe from transposed wunderground data...
# ... use ReGex to filter out unnessessary characters 


from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.functions import regexp_extract
from pyspark.sql.types import *

import os
import sys

os.chdir('C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Wunderground_Data\\transposed_data\\parquet_files')
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable


spark = SparkSession.builder.getOrCreate()
path = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Wunderground_Data\\transposed_data\\parquet_files\\'

'''
schema = StructType([ \
    StructField("date", StringType(), True), \
    StructField("time", StringType(), True), \
    StructField("temp", StringType(), True), \
    StructField("dew", StringType(), True), \
    StructField("humid", StringType(), True), \
    StructField("wind_direction", StringType(), True), \
    StructField("wind_speed", StringType(), True), \
    StructField("wind_gust", StringType(), True), \
    StructField("pressure", StringType(), True), \
    StructField("precip", StringType(), True), \
    StructField("condition", StringType(), True) \
])
'''

directory = 'C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Wunderground_Data/transposed_data/parquet_files/'
 
# iterate over files in directory, add to file list
file_list = []
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    file_list.append(directory+filename+'/'+'*.parquet')
    



### When writing with Schema, returns only nulls ###

df=spark.read.format('parquet').load(file_list)
df=df.toDF("date","time","temperature_°F","dew_°F","humidity_°F","wind_direction","wind_speed_mph","wind_gust_mph","pressure_inches","precip_inches","condition")
df=df.withColumn('temperature_°F',regexp_extract('temperature_°F','\d*\.?\d*',0))
df=df.withColumn('dew_°F',regexp_extract('dew_°F','\d*\.?\d*',0))
df=df.withColumn('humidity_°F',regexp_extract('humidity_°F','\d*\.?\d*',0))
df=df.withColumn('wind_speed_mph',regexp_extract('wind_speed_mph','\d*\.?\d*',0))
df=df.withColumn('wind_gust_mph',regexp_extract('wind_gust_mph','\d*\.?\d*',0))
df=df.withColumn('pressure_inches',regexp_extract('pressure_inches','\d*\.?\d*',0))
df=df.withColumn('precip_inches',regexp_extract('precip_inches','\d*\.?\d*',0))

df.show()

# worked! 
df.coalesce(1).write.format("parquet").mode("append").save('C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Pipeline_Prototype/DFs/wunderground_df.parquet')
