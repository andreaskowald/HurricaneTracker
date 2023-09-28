#!/usr/bin/python3

import os
import requests
import csv

# function to download station table
def download_station_table(data_url, destination_name):
    station_table_url= os.path.join(data_url,  destination_name)
    response = requests.get(station_table_url)
    if response.status_code == 200:
        with open("station_table.txt", "wb") as file:
            file.write(response.content)
        print(f"Downloaded data for station table")
    else:
        print(f"Failed to download data for station table")

# Iterate through buoys within the specified range
download_station_table("https://www.ndbc.noaa.gov/data/stations/", "../data/station_table.txt")
 
file_path = '../data/station_table.txt'
station_ids = []
 
delimiter = '|'
 
with open(file_path, 'r', newline='') as station_table:
    table_reader = csv.reader(station_table, delimiter=delimiter)
# extract station IDs from station table 
    for row in table_reader:
        station_ids.append(row[0])
