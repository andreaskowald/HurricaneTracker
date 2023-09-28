#!/usr/bin/python3

import os
import requests


# Define NOAA NDBC data URL
data_url = "https://www.ndbc.noaa.gov/data/realtime2/"

stationIDs = []

# Function to download buoy data files
def download_buoy_data(buoy_id):
    buoy_data_url = os.path.join(data_url, buoy_id + ".txt")
    response = requests.get(buoy_data_url)
    print(buoy_id)
    if response.status_code == 200:
        with open("../data" + buoy_id + ".txt", "wb") as file:
            file.write(response.content)
        print(f"Downloaded data for buoy {buoy_id}")
    else:
        print(f"Failed to download data for buoy {buoy_id}")

# Iterate through buoys within the specified range
for stationID in stationIDs:
    # Generate buoy ID based on latitude and longitude (example format)
    buoy_id = f"{stationID}"
    download_buoy_data(buoy_id)
