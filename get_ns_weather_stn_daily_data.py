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
foldername = 'NSDailyWeatherData'   
# URL of the the weather data download page.
url = 'https://dd.weather.gc.ca/climate/observations/daily/csv/NS/'
##########################################################################

ns.create_weather_stn_data_directory(foldername)
# Start the timer for the program.
start_time = time.time()
dataframes = ns.get_stn_data_concurrently(url, stn_id)
<<<<<<< Updated upstream
=======
# To compare run times between the concurrent and sequential function versions, 
# comment out the function above, remove the hash from the function below, 
# and run again.
# ns.get_stn_data_sequentially(url, stn_id)
# Concatenate the dataframes into one dataframe.
>>>>>>> Stashed changes
df = ns.concatenate_daily_dataframes(dataframes)
df_dropped = ns.drop_columns_daily_data(df)
<<<<<<< Updated upstream
regularly_spaced_df = ns.regularly_spaced_dates(df_dropped)
ns.check_if_regularly_spaced(regularly_spaced_df)
date_range = ns.get_date_range(regularly_spaced_df)
ns.save_cleaned_daily_data_to_csv(regularly_spaced_df, foldername, stn_id, date_range)
=======
# Check for duplicate dates.
ns.check_for_duplicate_dates_in_daily(df_dropped)
####### If there are duplicate dates, use the function below to drop them #####
# remove_duplicate_dates(df_dropped)
# # Make the days regularly spaced.
regularly_spaced_df = ns.regularly_spaced_dates_in_daily(df_dropped)
# # Check if the days are regularly spaced.
ns.check_if_days_regularly_spaced_for_daily_data(regularly_spaced_df)
# Get the date range of the data that will be used in the csv filename.
date_range = ns.get_date_range(regularly_spaced_df)
# Save the datframe to a csv file.
ns.save_cleaned_daily_data_to_csv(df_dropped, foldername, stn_id, date_range)
>>>>>>> Stashed changes
# End the timer for the program.
end_time = time.time()
print(f'Time taken: {end_time - start_time} seconds.')
print(df_dropped.head())