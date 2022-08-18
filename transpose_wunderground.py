# Robert Jones
# Transpose parquet files from scrape_wunderground.py. Take single column and break into multiple columns with named heads. 
# 6.22.22

import os
import sys
from pyspark.sql import SparkSession
import pandas as pd


os.chdir('C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Wunderground_Data\\transposed_data\\parquet_files')
os.environ["PYSPARK_PYTHON"] = sys.executable
os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

spark = SparkSession.builder.getOrCreate()

folder = 'C:\\Users\\Bob\\Desktop\\SpringBoard\\Python_Projects\\NYC_Taxi_Capstone\\Wunderground_Data\\weather_data'

# Iterate thru all files in folder
for filename in os.listdir(folder):
    f = os.path.join(folder,filename)
    if os.path.isfile(f):
        df = spark.read.format('parquet').load(f)

        data = []

        # Converts single column and splits into multiple
        for i in range(0,int(df.count()/10),1):
            time = df.collect()[0:df.count():10][i][1]
            temp = df.collect()[1:df.count():10][i][1]
            dew = df.collect()[2:df.count():10][i][1]
            humid = df.collect()[3:df.count():10][i][1]
            wind_direction = df.collect()[4:df.count():10][i][1]
            wind_speed = df.collect()[5:df.count():10][i][1]
            wind_gust = df.collect()[6:df.count():10][i][1]
            pressure = df.collect()[7:df.count():10][i][1]
            precip = df.collect()[8:df.count():10][i][1]
            condition = df.collect()[9:df.count():10][i][1]
            date = df.columns[1]
            
            # Heads
            vals = [date,time,temp,dew,humid,wind_direction,wind_speed,wind_gust,pressure,precip,condition]
            data.append(vals)

        df_pandas = pd.DataFrame(data)
        df_spark = spark.createDataFrame(df_pandas)
        df_spark.write.parquet(f'transposed_wunderground_{date}.parquet')
