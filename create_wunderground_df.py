# Robert Jones
# 7.18.22
# Create one dataframe from transposed wunderground data...
# ... use ReGex to filter out unnessessary characters 


from pyspark.sql import SparkSession
from pyspark.sql.types import *

import os
import sys

os.chdir('C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Wunderground_Data\\transposed_data\\parquet_files')
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable



spark = SparkSession.builder.getOrCreate()
path = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Wunderground_Data\\transposed_data\\parquet_files\\'
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


directory = 'C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Wunderground_Data/transposed_data/parquet_files/'
 
# iterate over files in directory, add to file list
file_list = []
for filename in os.listdir(directory):
    f = os.path.join(directory, filename)
    file_list.append(directory+filename+'/'+'*.parquet')
    



### When writing with Schema, returns only nulls ###

# df = spark.read.format('parquet').schema(schema).load(file_list)
# df.show()



### When inferring schema (all StringType), df builds

df = spark.read.option("header","false").format('parquet').load(file_list) # Works
df.show()

# df.coalesce(1).write.format("parquet").mode("append").save('C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Wunderground_Data/transposed_data/single_df_parquet/wunderground_df.parquet')
