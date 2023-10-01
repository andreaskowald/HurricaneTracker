import os
import requests
import csv
import re
import pandas as pd
import pickle

station_table = pd.read_parquet('../data/station_table.parquet')
stations = station_table['station_id']
data_url = 'https://www.ndbc.noaa.gov/data/realtime2/'

def download_buoy_data(buoy_id):
    buoy_data_url = os.path.join(data_url, buoy_id + ".txt")
    response = requests.get(buoy_data_url)
    if response.status_code == 200:
        with open("../data/buoys/txt/" + buoy_id + ".txt", "wb") as file:
            file.write(response.content)
        print(f"Downloaded data for buoy {buoy_id}")
        return True
    else:
        print(f"Failed to download data for buoy {buoy_id}")
        return False

active_stations = []
for station in stations:
    if download_buoy_data(station.upper()) == True:
        active_stations.append(station)    


def process_parc(station_id):
    df = pd.read_csv("../data/buoys/txt/" + station_id.upper() + ".txt", sep=r'\s+')
    df.to_parquet("../data/buoys/parquet/" + station_id.upper() + ".parquet")
    return df

buoy_dict = {}
for station in active_stations:
    buoy_dict[station] = process_parc(station)


def convert_columns_to_float(df):
    for column in df.columns:
        try:
            df[column] = df[column].astype(float)
        except ValueError:
            df[column] = pd.to_numeric(df[column], errors='coerce')
    return df



def convert_datetime(df):
    df.dropna(subset=['#YY', 'MM', 'DD'], inplace=True)
    try:
        df['#YY'] = df['#YY'].astype(int).astype(str)
        df['MM'] = df['MM'].astype(int).astype(str)
        df['DD'] = df['DD'].astype(int).astype(str)
        df['hh'] = df['hh'].astype(int).astype(str)
        df['mm'] = df['mm'].astype(int).astype(str)


        df['date'] = df['#YY'] + '-' + df['MM'] + '-' + df['DD'] + ' ' + df['hh'] + ':' + df['mm'] + ':00'

    ##df['hh'] = df['hh'].astype(int)
    ##df['mm'] = df['mm'].astype(int)


        df['timestamp'] = pd.to_datetime(df['date'])
        df = df.drop(columns=['#YY', 'MM', 'DD', 'date', 'hh', 'mm'])
        columns = ['timestamp'] + [col for col in df.columns if col != 'timestamp']
        return df[columns]
    except:
        return df


for buoy_id, buoy_df in buoy_dict.items():
    buoy_dict[buoy_id] = convert_columns_to_float(buoy_df)
    buoy_dict[buoy_id] = convert_datetime(buoy_df)


for buoy_id, buoy_df in buoy_dict.items():
    buoy_df = buoy_df.dropna(subset=['timestamp'])    
    buoy_dict[buoy_id] = buoy_dict[buoy_id].reset_index(drop=True)



pickle_filename = "../data/buoy_data.pkl"

with open(pickle_filename, 'wb') as file:
    pickle.dump(buoy_dict, file)    
