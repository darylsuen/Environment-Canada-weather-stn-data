# meteorological_data
Scripts for downloading Environment Canada weather station data from Nova Scotia weather stations.

Have not checked if these scripts work for weather stations in other provinces, yet.

The file 'ns_weather_stn_data_functions.py' has all the functions required and imported in the 'get_ns_weather_stn_daily_data.py' and 'get_ns_weather_stn_hourly_data.py' files. Therefore, if downloading files separately, the 'ns_weather_stn_data_function.py' file must be downloaded to the same directory as the other files for the scripts to work.

These are function-based scripts. A potentially more useful version may be to create a class and make the functions methods, instead. There might be useful class properties that can be added.
