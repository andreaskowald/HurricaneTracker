import os
import requests
import csv
import re
import pandas as pd

# https://www.ndbc.noaa.gov/data/stations/

# function to download station table
def download_station_table(data_url, destination_name):
    station_table_url= os.path.join(data_url,  destination_name)
    response = requests.get(station_table_url)
    if response.status_code == 200:
        with open("../data/station_table.txt", "wb") as file:
            file.write(response.content)
        print(f"Downloaded data for station table")
    else:
        print(f"Failed to download data for station table")

with open("../data/station_table.txt", "r") as file:
    data = file.read()A

pattern = r'([A-Za-z0-9]+)\|([^|]*)\|([^|]*)\|([^|]*)\|([^|]*)\|([^|]*)\|.*?(\d+\.\d+)\s*([NS])\s*(\d+\.\d+)\s*([EW]).*?\|([^|]*)\|([^|]*)\|([^|^\n]*)'

# Use regular expressions to find all matches
matches = re.findall(pattern, data)

# Create a list to store the data
data_list = []

# Convert the matches into a list of dictionaries
for station_id, owner, ttype, hull, name, payload, lat, lat_dir, lon, lon_dir, timezone, forecast, note in matches:
    latitude = float(lat) if lat_dir == 'N' else -float(lat)
    longitude = float(lon) if lon_dir == 'E' else -float(lon)
    data_list.append({
        'station_id': station_id,
        'owner': owner,
        'ttype': ttype,
        'hull': hull,
        'name': name,
        'payload': payload,
        'latitude': latitude,
        'longitude': longitude,
        'timezone': timezone,
        'forecast': forecast,
        'note': note
    })

# Create a Pandas DataFrame from the list of dictionaries
df = pd.DataFrame(data_list)

df.to_parquet('../data/station_table.parquet')

