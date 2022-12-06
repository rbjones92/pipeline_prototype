# Robert Jones 
# 6.16.2022
# Scraping wunderground website for weather data in New York.

import datetime
from bs4 import BeautifulSoup as BS
from selenium import webdriver
from webdriver_manager.chrome import ChromeDriverManager
import pandas as pd
import time
import logging

# Create logfile to catch exceptions
logging.basicConfig(filename="logfile.log", level=logging.DEBUG)
LOGGING = logging.getLogger()

# Function to find ranges of dates
def get_dates():
    # Input desired range of dates
    d1 = datetime.date(2022,1,1)
    d2 = datetime.date(2022,2,1)
    dd = [d1 + datetime.timedelta(days=x) for x in range((d2-d1).days + 1)]

    date_list = []
    for d in dd:
        date_list.append(str(d))
    return date_list


# function to load wunderground data page source
def render_page(url):
    driver = webdriver.Chrome(ChromeDriverManager().install())
    driver.get(url)
    time.sleep(3)
    r = driver.page_source
    driver.quit()
    return r


# function to scrape wunderground
def scraper(page, dates):

    # append date to wunderground URL to find desired webpage
    for d in dates:
        url = str(str(page) + str(d))
        r = render_page(url)
        soup = BS(r, "html.parser")
        container = soup.find('lib-city-history-observation')
        check = container.find('tbody')

        data = []
        # Use try-except block to search in HTML for needed data.
        try:
            for c in check.find_all('tr', class_='ng-star-inserted'):
                for i in c.find_all('td', class_='ng-star-inserted'):
                    trial = i.text
                    trial = trial.strip('  ')
                    data.append(trial)
    

            df_daily = []
            # Loop thru data, name it, and append to dataframe
            for i in range(0,len(data),10):
                df = pd.DataFrame(data[i:i+10],columns=[d],index=['Time','Temperature','Dew_Point','Humidity','Wind','Wind_Speed','Wind_Gust','Pressure','Precipitation','Condition'])
                df_daily.append(df)

            # Concatenate dataframes in variable df_daily
            df_daily = pd.concat(df_daily)
            # Write to parquet. May need to pip install pyarrow 
            df_daily.to_parquet(f'NY_Weather{d}.parquet')
        
        # catch errors and log them, continue processing data
        except AttributeError as e:
            LOGGING.error(f"Error at {e}".format(datetime.datetime.now()),exc_info=1)
            continue

# Start with producing desired dates
dates = get_dates()
# Input URL for desired wunderground station
page = 'https://www.wunderground.com/history/daily/us/ny/new-york-city/KLGA/date/'
# Start scraper function
df = scraper(page, dates)
