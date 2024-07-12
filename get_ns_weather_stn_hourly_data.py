import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import time
import concurrent.futures
import ns_weather_stn_data_functions as ns


######################## USER INPUT ######################################
# Choose a station id.
stn_id = '8200015'
# Choose a folder name to save the daily weather data.
foldername = 'NsHourlyWeather' 
# URL of the the weather data download page.
url = 'https://dd.weather.gc.ca/climate/observations/hourly/csv/NS/'
##########################################################################

ns.create_weather_stn_data_directory(foldername)
# Start the timer for the program.
start_time = time.time()
dataframes = ns.get_stn_data_concurrently(url, stn_id)
df = ns.concatenate_hourly_dataframes(dataframes)
df_dropped = ns.drop_columns_hourly_data(df)
regularly_spaced_df = ns.regularly_spaced_hours(df_dropped)
ns.check_if_days_regularly_spaced_for_hourly_data(regularly_spaced_df)
date_range = ns.get_date_range(regularly_spaced_df)
ns.save_cleaned_hourly_data_to_csv(regularly_spaced_df, foldername, stn_id, date_range)
# End the timer for the program.
end_time = time.time()
print(f'Time taken: {end_time - start_time} seconds.')
print(df_dropped.head())