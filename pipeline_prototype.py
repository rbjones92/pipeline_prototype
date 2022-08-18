# Robert Jones
# 8.18.22
# Prototyping a New York Taxi and Weather Data Pipeline

import os
import logging

os.chdir('C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Pipeline_Prototype')


class PipelinePrototype():

    try:

        # Grab new york data from website
        import get_nyc_csv_data
        print('Finishing grabbing taxi data')

        # Create dataframes for nyc_taxi_data
        import create_nyc_dfs
        print('Finished creating New York taxi dataframes (green,yellow,high volume,for-hire')

        # Grab daily weather data (2009-2020)
        import selenium_scrape_daily_wunderground
        print("Finished grabbing wunderground weather data")

        # Transpose weather data (one column to many columns)
        import transpose_wunderground
        print('Finished transposing weather data')

        # Create dataframe for weather data
        import create_wunderground_df
        print('Finished creating weather dataframe')

    except ModuleNotFoundError as Argument:
        logging.exception("Error has occured creating data pipeline, could not find modules")
        f = open('C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Pipeline_Prototype/logging/log_file.txt','a')
        f.write(str(Argument))
        f.close()
        

    



        

    

