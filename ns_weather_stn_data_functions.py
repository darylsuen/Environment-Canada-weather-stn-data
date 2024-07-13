import requests
from bs4 import BeautifulSoup
import os
import pandas as pd
import time
import concurrent.futures


def create_weather_stn_data_directory(foldername: str) -> str:
    """
    Function to create a directory withint the working directory to save the
    daily weather data.
    """
    cwd = os.getcwd()
    daily_dir_path = os.path.join(cwd, foldername)
    os.makedirs(daily_dir_path, exist_ok=True)
    return foldername

def get_stn_data_sequentially(url: str, stn_id: str) -> pd.DataFrame:
    """
    A function to scrape the webpage and download the CSV files for daily
    weather data for a station. The CSV files are encoded in ISO-8859-1. This
    function may be less efficient as it downloads the CSV files sequentially.
    """
    
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    dataframes = []
    for link in links:
        if stn_id in link.get('href'):
            filename = link.get('href')
            full_url = url.rsplit('/', 1)[0] + '/' + filename
            df = pd.read_csv(full_url, encoding='ISO-8859-1')
            dataframes.append(df)
    return dataframes

def download_and_read_csv(url: str, filename: str) -> pd.DataFrame:
    """
    Download the CSV file and read it into a pandas dataframe. The CSV files
    from the government bulk download site are encoded in ISO-8859-1.
    """
    full_url = url.rsplit('/', 1)[0] + '/' + filename
    df = pd.read_csv(full_url, encoding='ISO-8859-1')
    return df

def get_stn_data_concurrently(url: str, stn_id: str) -> pd.DataFrame:
    """
    Scrape the webpage and download the CSV files for the station. Use 
    ThreadPoolExecutor to download the CSV files concurrently for efficiency.
    Return a list of dataframes of all the CSV files for the station.
    """
    # Scrape the webpage and download the CSV files for the station.
    response = requests.get(url)
    soup = BeautifulSoup(response.text, 'html.parser')
    links = soup.find_all('a')
    filenames = [link.get('href') for link in links if stn_id in link.get('href')]
    
    dataframes = []
    # Use ThreadPoolExecutor to download the CSV files concurrently and create
    # a dictionary of future objects.
    with concurrent.futures.ThreadPoolExecutor(max_workers=20) as executor:
        future_obj_dict = {
            executor.submit(download_and_read_csv, url, filename): filename 
            for filename in filenames
            }
        # Use as_completed to iterate over the future objects as they are completed
        # and get the results, which are the dataframes.
        for future_obj in concurrent.futures.as_completed(future_obj_dict):
            df = future_obj.result()
            # Check if the dataframe is empty (excluding the header) and exclude
            # it from the list of dataframes if it is empty.
            if not df.empty:
                dataframes.append(df)
    return dataframes
    
def concatenate_daily_dataframes(dataframes: list)-> pd.DataFrame:
    """
    Concatenate the downloaded dataframes from the bulk download site and sort
    the data by date. This function can only be used for DAILY weather data.
    """ 
    concatenated = pd.concat(dataframes)
    sorted_df = concatenated.sort_values(by='Date/Time')
    return sorted_df

def concatenate_hourly_dataframes(dataframes: list)-> pd.DataFrame: 
    """
    Concatenate the downloaded dataframes from the bulk download site and sort
    the data by date. This function can only be used for HOURLY weather data.
    """ 
    concatenated = pd.concat(dataframes)
    sorted_df = concatenated.sort_values(by='Date/Time (LST)')
    return sorted_df

def convert_LST_to_UTC(df: pd.DataFrame) -> pd.DataFrame:
    # Convert Date/Time (LST) to datetime.
    df['Date/Time (LST)'] = pd.to_datetime(df['Date/Time (LST)'])
    # Convert Date/Time (LST) to datetime UTC
    df['Date/Time'] = df['Date/Time (LST)'].dt.tz_localize(
    'America/Toronto', nonexistent='shift_forward', ambiguous='NaT'
    ).dt.tz_convert('UTC')
    df_time_converted = df
    return df_time_converted

def drop_columns_daily_data(df: pd.DataFrame) -> pd.DataFrame:
    """
    Drop columns that are not needed for the LSTM model. Many of these columns
    are flags and usually have many missing values.
    """
    df_dropped = df.drop(columns=[
         'Longitude (x)', 'Latitude (y)', 'Station Name', 'Year', 'Month', 
         'Day', 'Data Quality', 'Max Temp Flag', 'Min Temp Flag', 
         'Mean Temp Flag', 'Heat Deg Days Flag', 'Cool Deg Days Flag', 
         'Total Rain Flag', 'Total Snow Flag', 'Total Precip Flag', 
         'Snow on Grnd Flag', 'Dir of Max Gust Flag', 'Spd of Max Gust Flag'
         ])
    return df_dropped

def drop_columns_hourly_data(df: pd.DataFrame) -> pd.DataFrame:
    df_dropped = df.drop(
        columns=['Longitude (x)', 'Latitude (y)', 'Station Name', 'Year', 
                 'Month', 'Day', 'Temp Flag', 'Dew Point Temp Flag', 
                 'Rel Hum Flag', 'Precip. Amount (mm)', 'Precip. Amount Flag', 
                 'Wind Dir Flag', 'Visibility (km)', 'Visibility Flag', 
                 'Stn Press Flag', 'Hmdx','Hmdx Flag', 'Wind Chill', 
                 'Wind Chill Flag', 'Weather', 'Wind Dir (10s deg)', 
                 'Wind Spd (km/h)', 'Wind Spd Flag', 'Stn Press (kPa)', 
                 'Time (LST)', 'Date/Time (LST)'] 
                 )
    return df_dropped

def check_for_duplicate_dates_in_daily(df: pd.DataFrame) -> None:
    """
    Check if there are duplicate dates in the dataframe. If there are, the
    functions that ensure the dates or hours are regularly spaced will not work.
    """
    unique_dates_count = df['Date/Time'].nunique()
    total_rows = len(df)
    try:
        assert unique_dates_count == total_rows
    except AssertionError:
        duplicate_dates = df[df['Date/Time'].duplicated(keep=False)]['Date/Time'].unique()
        for date in duplicate_dates:
            print(date)
        raise ValueError('There are duplicate dates in the dataframe.')   
    
def print_duplicate_dates_in_daily(df: pd.DataFrame) -> None:
    """
    Print the duplicate dates in the dataframe.
    """
    duplicate_dates = df[df['Date/Time'].duplicated(keep=False)]['Date/Time'].unique()
    for date in duplicate_dates:
        print(date)

def remove_duplicate_dates_in_daily(df: pd.DataFrame) -> pd.DataFrame:
    """
    Remove duplicate dates from the dataframe.
    """
    df_no_duplicates = df.drop_duplicates(subset='Date/Time', keep='first')
    return df_no_duplicates

def regularly_spaced_dates_in_daily(df: pd.DataFrame) -> pd.DataFrame: 
    """
    Ensure that the dates are regularly spaced. For LSTM models, the dates
    should be regularly spaced.
    """   
    df['Date/Time'] = pd.to_datetime(df['Date/Time'])
    df = df.rename(columns={'Date/Time': 'date'})
    df.set_index('date', inplace=True)  
    df_regularly_spaced = df.asfreq('D')
    return df_regularly_spaced

def regularly_spaced_hours(df: pd.DataFrame) -> pd.DataFrame:
    """
    Ensure that the hourly intervals are regularly spaced.
    """   
    df.set_index('Date/Time', inplace=True) 
    # There may be duplicated rows. 
    df = df[~df.index.duplicated(keep='first')]    
    df_regularly_spaced = df.asfreq('h')
    return df_regularly_spaced

def check_if_days_regularly_spaced_for_daily_data(df: pd.DataFrame) -> None:
    """
    Check if the dates are regularly spaced. For LSTM models, the dates should 
    be regularly spaced.
    """
    date_diff = df.index.to_series().diff().dt.days
    if (date_diff > 1).any():
        raise ValueError('Dates are not regularly spaced.')
    else:
        print('Dates are regularly spaced.')

def check_if_regularly_spaced_for_hourly_data(df: pd.DataFrame) -> None:
    """
    Check if the hours are regularly spaced.
    """
    hour_diff = df.index.to_series().diff().map(lambda x: x.total_seconds() / 3600)
    if (hour_diff > 1).any():
        raise ValueError('Hours are not regularly spaced.')
    else:
        print('Hours are regularly spaced.')

def get_date_range(df: pd.DataFrame) -> str:
        date_range = (
        f'{df.index.min().strftime('%Y%m%d')}-{df.index.max().strftime('%Y%m%d')}'
        ) 
        return date_range
    
def save_cleaned_daily_data_to_csv(df: pd.DataFrame, foldername: str, stn_id: str, date_range) -> None:
    # Save the cleaned data to a CSV file.
    cwd = os.getcwd()
    daily_dir_path = os.path.join(cwd, foldername)
    filename = os.path.join(daily_dir_path, stn_id + '_' + date_range + 'daily.csv')
    df.to_csv(filename)

def save_cleaned_hourly_data_to_csv(df: pd.DataFrame, foldername: str, stn_id: str, date_range: str) -> None:
    # Save the cleaned data to a CSV file.
    cwd = os.getcwd()
    dir_path = os.path.join(cwd, foldername)
    filename = os.path.join(dir_path, stn_id + '_' + date_range + 'hourly.csv')
    df.to_csv(filename)
    





