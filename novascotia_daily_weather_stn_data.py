import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import time
import concurrent.futures


def create_daily_weather_folder(foldername: str) -> str:
    # Create a directory to save the CSV files.
    cwd = os.getcwd()
    daily_dir_path = os.path.join(cwd, foldername)
    os.makedirs(daily_dir_path, exist_ok=True)
    return foldername

# def get_stn_daily_data(url: str, stn_id: str) -> pd.DataFrame:
#     # Scrape the webpage and download the CSV files for the station.
#     response = requests.get(url)
#     soup = BeautifulSoup(response.text, 'html.parser')
#     links = soup.find_all('a')
#     dataframes = []
#     for link in links:
#         if stn_id in link.get('href'):
#             filename = link.get('href')
#             full_url = url.rsplit('/', 1)[0] + '/' + filename
#             df = pd.read_csv(full_url, encoding='ISO-8859-1')
#             dataframes.append(df)
#     return dataframes

def download_and_read_csv(url: str, filename: str) -> pd.DataFrame:
    full_url = url.rsplit('/', 1)[0] + '/' + filename
    df = pd.read_csv(full_url, encoding='ISO-8859-1')
    return df

def get_stn_daily_data(url: str, stn_id: str) -> pd.DataFrame:
    # Scrape the webpage and download the CSV files for the station.
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    filenames = [link.get('href') for link in links if stn_id in link.get('href')]
    
    dataframes = []
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_to_df = {executor.submit(download_and_read_csv, url, filename): filename for filename in filenames}
        for future in concurrent.futures.as_completed(future_to_df):
            df = future.result()
            dataframes.append(df)
    return dataframes
    
def concatenate_dataframes(dataframes: list)-> pd.DataFrame: 
    concatenated = pd.concat(dataframes)
    sorted_df = concatenated.sort_values(by='Date/Time')
    return sorted_df

def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_dropped = df.drop(columns=[
         'Longitude (x)', 'Latitude (y)', 'Station Name', 'Year', 'Month', 
         'Day', 'Data Quality', 'Max Temp Flag', 'Min Temp Flag', 
         'Mean Temp Flag', 'Heat Deg Days Flag', 'Cool Deg Days Flag', 
         'Total Rain Flag', 'Total Snow Flag', 'Total Precip Flag', 
         'Snow on Grnd Flag', 'Dir of Max Gust Flag', 'Spd of Max Gust Flag'
         ])
    return df_dropped

def regularly_spaced_dates(df: pd.DataFrame) -> pd.DataFrame:    
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    df = df.rename(columns={'Date/Time': 'date'})
    df.set_index('date', inplace=True)  
    df_regularly_spaced = df.asfreq('D')
    return df_regularly_spaced

def check_if_regularly_spaced(df: pd.DataFrame) -> None:
    # Check if the dates are regularly spaced.
    date_diff = df.index.to_series().diff().dt.days
    if (date_diff > 1).any():
        raise ValueError('Dates are not regularly spaced.')
    else:
        print('Dates are regularly spaced.')

def get_date_range(df: pd.DataFrame) -> str:
        date_range = (
        f'{df.index.min().strftime('%Y%m%d')}-{df.index.max().strftime('%Y%m%d')}'
        ) 
        return date_range
    
def save_cleaned_daily_to_csv(df: pd.Dataframe, foldername: str, stn_id: str, date_range: str) -> None:
    # Save the cleaned data to a CSV file.
    cwd = os.getcwd()
    daily_dir_path = os.path.join(cwd, foldername)
    filename = os.path.join(daily_dir_path, stn_id + '_' + date_range + '.csv')
    df.to_csv(filename)
    
def main() -> None:
    # Choose a station id.
    stn_id = '8200015'
    # Choose a folder name to save the daily weather data.
    foldername = create_daily_weather_folder('NsDailyWeather')
    start_time = time.time()
    # URL of the the weather data download page.
    url = 'https://dd.weather.gc.ca/climate/observations/daily/csv/NS/'
    dataframes = get_stn_daily_data(url, stn_id)
    df = concatenate_dataframes(dataframes)
    df_dropped = drop_columns(df)
    regularly_spaced_df = regularly_spaced_dates(df_dropped)
    check_if_regularly_spaced(regularly_spaced_df)
    date_range = get_date_range(regularly_spaced_df)
    save_cleaned_daily_to_csv(regularly_spaced_df, foldername, stn_id, date_range)
    end_time = time.time()
    print(f'Time taken: {end_time - start_time} seconds.')
    print(df_dropped.head())

main()


