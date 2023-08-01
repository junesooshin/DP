import numpy as np
import pandas as pd
import delta_sharing as ds
import matplotlib.pyplot as plt


def load_delta_sharing(limit, save, save_name):
    """
    Function to load data from delta_sharing .share file as df and option to save in pickle format
    Input: limit (don't set to get all the data), save(Boolean), save file location
    Output: df and saved pickle
    """ 
    profile_file = r"C:\Users\junes\Desktop\BEOF\config.share"
    client = ds.SharingClient(profile_file)
    # print(client.list_all_tables()) # To check the tables available

    #"#<share-name>.<schema-name>.<table-name>"
    table_url = profile_file + "#dtu-data-privacy-study.gold.heat_consumption"
    df = ds.load_as_pandas(table_url, limit=limit)

    if save == True:
        #df.to_pickle("./data/Ostermarie.pkl")
        df.to_pickle(f"./data/{save_name}.pkl")
        
    return df

def split_cities(df_all, city, save):
    """
    Function to extract cities and save
    Input: df_all, city name, save(Boolean)
    Output: df_city and saved pickle
    """
    # Ostermarie_df = df_all[df_all.city == 'Østermarie'] #215 meters
    df_city = df_all[df_all.city == city]

    if save == True:
        # Ostermarie_df.to_pickle("./data/Ostermarie.pkl")
        df_city.to_pickle(f"./data/{city}.pkl")
    
    return df_city


# df_update = ds.load_table_changes_as_pandas(table_url, starting_version=0, ending_version=2)
# df_update2 = ds.load_table_changes_as_pandas(table_url, starting_timestamp=160223, ending_timestamp=170223)
# df_update3 = ds.load_table_changes_as_pandas(table_url, starting_timestamp="2023-01-01", ending_timestamp="2023-02-01")

def select_data(city, start_date, end_date, granularity):
    """
    Function to select data for a particulr city in selected time horizon (also ones that have daily data)
    Input: city, start date, end date, granularity
    Output: df
    """
    df = pd.read_pickle(f"./data/{city}.pkl")
    df1 = df[df["reading_datetime"] >= start_date][df["reading_datetime"] <= end_date]

    #Take meter type "mesh" instead of "M-bus" so that the data is daily instead of hourly
    #Hourly data doesn't change so much, so we use daily so that each data point adds more value
    if granularity == 'daily':
        df2 = df1[df1.heat_meter_connection == 'mesh']
    elif granularity == 'hourly':
        df2 = df1[df1.heat_meter_connection == 'M-bus']
    df3 = df2.sort_values(by='reading_datetime')

    return df3

def fill_outliers(array, lim):
    df = pd.DataFrame(array)
    df1 = df.copy()
    outliers = np.abs(df1.diff()) > lim
    df1[outliers] = np.nan
    df1_filled = df1.interpolate(method='linear',limit=6, limit_direction='both')
    return df1_filled.values.flatten()

def temp_loss(meter_data, l, R_in, meter_no, t, meter_avg_temp):
    T_soil = 10
    r_out = 110/(2*1000)
    Cp = 1.1622 # 4184 J/kg.K = 1.1622 Wh/kg.K
    k = 0.025 # W/(m*K) #https://smartenergysystems.eu/wp-content/uploads/2019/04/thrid_-_oliver_martin-du_pan.pdf
    T_meter = meter_avg_temp[meter_no][t-1]
    ΔT = T_meter - T_soil # C
    L = l[meter_no] # m 
    r_in = R_in[meter_no] # m

    Q = 2*np.pi*L*k*(ΔT)/np.log(r_out/r_in) # W

    mf = (meter_data[meter_no]['cumulative_hot_water_volume'].values[t]-meter_data[meter_no]['cumulative_hot_water_volume'].values[t-1])*1000 #kg/h
    T_node = (Q / (Cp * mf)) + T_meter

    return Q, T_meter, T_node

def calculate_sensitivity(data):
  """Calculates the sensitivity of a time-series data.

  Args:
    data: A list of numbers representing the time-series data.

  Returns:
    The sensitivity of the time-series data.
  """

  max_diff = 0
  for i in range(len(data)):
    for j in range(i + 1, len(data)):
      diff = abs(data[i] - data[j])
      if diff > max_diff:
        max_diff = diff

  return max_diff

def fill_outliers(array, lim):
    df = pd.DataFrame(array)
    df1 = df.copy()
    outliers = np.abs(df1.diff()) > lim
    df1[outliers] = np.nan
    df1_filled = df1.interpolate(method='linear',limit=6, limit_direction='both')
    return df1_filled.values.flatten()