# Robert Jones
# 2/1/2022
# Capstone Project w/ Springboard
# Scraping a website to download csv 

import requests # For DLing HTML
from bs4 import BeautifulSoup as bs # To work with HTML
import logging # to log potential errors
import os # to work with windows filesystem


class NYC_CSV_data():

    # website URLs to scrape
    URL = 'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
    FILETYPE = '.csv'

    # function to parse website
    def get_soup():

        # website URLs to scrape
        URL = 'https://www1.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
        return bs(requests.get(URL).text, 'html.parser')


    try:

        os.chdir('C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/New_York_Taxi_Data')

        # find all 'a' tags
        for link in get_soup().find_all('a'):

            # look for links within a tags
            csv_link = link.get('href')
            # if filetype (.csv) is in link then...
            if FILETYPE in csv_link:
                print('Downloading:',link.text,'.csv')
                # save file to hard drive 
                with open(link.text, 'wb') as file:
                    response = requests.get(csv_link)
                    file.write(response.content)


    # Log errors
    except Exception as Argument:
        logging.exception("Error has occured grabbing taxi data, please check get_nyc_csv_data.py")
        f = open('C:/Users/Bob/Desktop/SpringBoard/Python_Projects/NYC_Taxi_Capstone/Pipeline_Prototype/logging/log_file.txt','a')
        f.write(str(Argument))
        f.close()

