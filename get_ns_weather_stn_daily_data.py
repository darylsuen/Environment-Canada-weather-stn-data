import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import time
import concurrent.futures
import ns_weather_stn_data_functions as ns


######################## USER INPUT ######################################
# Choose a station id.
stn_id = '8200091'
# Choose a folder name to save the daily weather data.
foldername = 'NSDailyWeatherData'   
# URL of the the weather data download page.
url = 'https://dd.weather.gc.ca/climate/observations/daily/csv/NS/'
##########################################################################

ns.create_weather_stn_data_directory(foldername)
# Start the timer for the program.
start_time = time.time()
# Use ThreadPoolExecutor to download the CSV files concurrently for higher efficiency.
dataframes = ns.get_stn_data_concurrently(url, stn_id)
# To compare run times between the concurrent and sequential function versions, 
# comment out the function above, remove the hash from the function below, 
# and run again.
#ns.get_stn_data_sequentially(url, stn_id)
# Concatenate the dataframes into one dataframe.
df = ns.concatenate_daily_dataframes(dataframes)
# Drop columns that are not needed. Edit function in source script to change 
# columns to drop.
df_dropped = ns.drop_columns_daily_data(df)
# Make the days regularly spaced.
regularly_spaced_df = ns.regularly_spaced_dates(df_dropped)
# Check if the days are regularly spaced.
ns.check_if_days_regularly_spaced_for_daily_data(regularly_spaced_df)
# Get the date range of the data that will be used in the csv filename.
date_range = ns.get_date_range(regularly_spaced_df)
# Save the datframe to a csv file.
ns.save_cleaned_daily_data_to_csv(regularly_spaced_df, foldername, stn_id, date_range)
# End the timer for the program.
end_time = time.time()
# Print the amount of time taken to run the program.
print(f'Time taken: {end_time - start_time} seconds.')
# Print the first few rows of the cleaned data.
print(df_dropped.head())