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

def drop_columns(df: pd.DataFrame) -> pd.DataFrame:
    df_dropped = df.drop(columns=['Longitude (x)', 'Latitude (y)', 'Station Name',
                              'Year', 'Month', 'Day', 'Temp Flag', 'Dew Point Temp Flag',
                              'Rel Hum Flag', 'Precip. Amount (mm)', 'Precip. Amount Flag',
                              'Wind Dir Flag', 'Visibility (km)', 'Visibility Flag',
                              'Stn Press Flag', 'Hmdx','Hmdx Flag', 'Wind Chill',
                              'Wind Chill Flag', 'Weather', 'Wind Dir (10s deg)', 
                              'Wind Spd (km/h)', 'Wind Spd Flag', 'Stn Press (kPa)',
                              'Time (LST)', 'Date/Time (LST)'] )
    return df_dropped

def regularly_spaced_hours(df: pd.DataFrame) -> pd.DataFrame:
    df.set_index('Date/Time', inplace=True) 
    # There may be duplicated rows. 
    df = df[~df.index.duplicated(keep='first')]    
    df_regularly_spaced = df.asfreq('h')
    return df_regularly_spaced

def check_if_regularly_spaced_hours(df: pd.DataFrame) -> None:
    # Check if the hours are regularly spaced.
    hour_diff = df.index.to_series().diff().map(lambda x: x.total_seconds() / 3600)
    if (hour_diff > 1).any():
        raise ValueError('Hours are not regularly spaced.')
    else:
        print('Hours are regularly spaced.')

def regularly_spaced_days(df: pd.DataFrame) -> pd.DataFrame:    
    df_regularly_spaced = df.asfreq('D')
    return df_regularly_spaced

def check_if_days_regularly_spaced(df: pd.DataFrame) -> None:
    # Check if the dates are regularly spaced.
    date_diff = df.index.to_series().diff().dt.days
    if (date_diff > 1).any():
        raise ValueError('Days are not regularly spaced.')
    else:
        print('Days are regularly spaced.')

def get_date_range(df: pd.DataFrame) -> str:
        date_range = (
        f'{df.index.min().strftime('%Y%m%d')}-{df.index.max().strftime('%Y%m%d')}'
        ) 
        return date_range
    
def save_cleaned_daily_to_csv(
        df: pd.DataFrame, foldername: str, stn_id: str, date_range: str
        ) -> None:
    # Save the cleaned data to a CSV file.
    cwd = os.getcwd()
    daily_dir_path = os.path.join(cwd, foldername)
    filename = os.path.join(daily_dir_path, stn_id + '_' + date_range + '.csv')
    df.to_csv(filename)
    
def main() -> None:
    # Choose a weather station ID.
    stn_id = '8200091'
    # Choose a folder name to save the CSV files.
    foldername = create_daily_weather_folder('NsHourlyWeather')
    start_time = time.time()
    # URL of the the weather data download page.
    url = 'https://dd.weather.gc.ca/climate/observations/hourly/csv/NS/'
    # Station ID of the weather station of interest.     
    dataframes = get_stn_daily_data(url, stn_id)
    df = concatenate_dataframes(dataframes)
    df_time_converted = convert_LST_to_UTC(df)
    df_dropped = drop_columns(df_time_converted)
    regularly_spaced_hours_df = regularly_spaced_hours(df_dropped)
    check_if_regularly_spaced_hours(regularly_spaced_hours_df)
    regularly_spaced_days_df = regularly_spaced_days(regularly_spaced_hours_df)
    check_if_days_regularly_spaced(regularly_spaced_days_df)
    date_range = get_date_range(regularly_spaced_days_df)
    save_cleaned_daily_to_csv(regularly_spaced_days_df, foldername, stn_id, date_range)
    end_time = time.time()
    print(f'Time taken: {end_time - start_time} seconds.')
    print(regularly_spaced_days_df.head())

main()