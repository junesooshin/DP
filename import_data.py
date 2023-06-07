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

def load_data(df, meter_range, num_days):
    """
    Function to return load, mass flow, temperature of selected df.
    If there are missing values, the meter range should be changed.
    Input: df, meter range, number of days
    Output: load df, mf df, t df
    """
    #Select meters
    meters = df.heat_meter_serial_number.unique()[meter_range]
    #Populate meter data
    meter_data = {}
    for i in range(0,len(meter_range)):
        df_select = df[df.heat_meter_serial_number == meters[i]]
        if df_select.shape[0] < num_days: #if there is missing days, change the range
            print(f'Selected meter {meters[i]} has missing data. Change the range of meter selection')
            break
        elif df_select.shape[0] >= num_days: #if days are repeated, use average 
            df_select_resample = df_select.resample('D', on='reading_datetime').mean()
            meter_data[i] = {'load': df_select_resample.cumulative_heat_energy.values, 
                            'supply_temp': df_select_resample.hot_water_supply_temperature.values,
                            'return_temp': df_select_resample.hot_water_return_temperature.values,
                            'cum_water_volume': df_select_resample.cumulative_hot_water_volume.values,
                            'cum_water_mass': df_select_resample.cumulative_hot_water_volume.values*1000}
    
    return meter_data

def plot_meter_data(meter_data, meter_no, days):
    # create figure and axes
    fig, (ax1, ax2, ax3) = plt.subplots(nrows=3, ncols=1, sharex=True)

    #First axis
    x = [i for i in range(0,days)] 
    ax1.plot(x, meter_data[meter_no]['load'], label='Load', marker='o', markersize=3)
    ax1.set_ylabel('Load over day [MWh]')
    ax1.set_title(f'Meter {meter_no}')
    ax1.legend(fontsize=7)

    #Second axis
    ax2.plot(x, meter_data[meter_no]['supply_temp'], label='supply temp', marker='o', markersize=3)
    ax2.plot(x, meter_data[meter_no]['return_temp'], label='return temp', marker='o', markersize=3)
    ax2.plot(x, meter_data[meter_no]['supply_temp']-meter_data[meter_no]['return_temp'], label='diff', marker='o', markersize=3)
    ax2.set_ylabel('Temperature [C]')
    ax2.legend(fontsize=7)

    #Third axis
    ax3.plot(x, meter_data[meter_no]['cum_water_mass'], label='mf', marker='o', markersize=3)
    ax3.set_ylabel('Mass flow [kg/day]')
    ax3.set_xlabel('Days')
    ax3.legend(fontsize=7)

    plt.plot()

