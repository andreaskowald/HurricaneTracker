import os
import pandas as pd
import holoviews as hv
import panel as pn

from holoviews.element import tiles
from colorcet import bmy

pn.extension('tabulator', template='fast')
import hvplot.pandas
import pickle

from holoviews.element.tiles import lon_lat_to_easting_northing
from holoviews.streams import Selection1D
from holoviews.plotting.links import DataLink


@pn.cache
def load_stations():
    stations = pd.read_parquet('../data/station_table.parquet')
    return stations

stations = load_stations()
stations['station_id'] = stations['station_id'].str.upper()

def load_buoy_data():
    with open("../data/buoy_data.pkl", 'rb') as file:
        buoy_dict = pickle.load(file)
    return buoy_dict

buoy_dict = load_buoy_data()

buoy_dict = {key: value for key, value in buoy_dict.items() if key in stations['station_id'].values}
mask = stations['station_id'].isin(buoy_dict.keys())
stations = stations[mask]



instruction = pn.pane.Markdown("""
This dashboard visualizes all NOAA weather buoys and allows exploring the relationships
between their local water temperature and variables such as their wind speed, gust speed, and wave height.
<br><br>Box- or lasso-select on each plot to subselect and hit the 
"Clear selection" button to reset. See the notebook source code for how to build apps
like this!""", width=600)

panel_logo = pn.pane.SVG(
   'https://upload.wikimedia.org/wikipedia/commons/7/79/NOAA_logo.svg',
    link_url='https://en.m.wikipedia.org/wiki', height=95, align='center'
)


intro = pn.Row(
    panel_logo,
    instruction,
    pn.layout.HSpacer(),
    sizing_mode='stretch_width'
)

intro


geo = stations.hvplot.points(
    'longitude', 'latitude', rasterize=False, tools=['hover', 'tap'], tiles='ESRI', cmap=bmy, colorbar=True,
    xaxis=None, yaxis=False, ylim=(-180, 180), min_height=180, responsive=True, size=40, color='purple', width=800, height=800,
    hover_cols=['name', 'ttype', 'note', 'station_id']
).opts('Tiles', alpha=0.8)

tap_stream = Selection1D(source=geo)


def click_callback(index):
    # Find the nearest point to the clicked position
    #Display the key (label) of the clicked point
    
    #key = clicked_point.data[0]
    if not index:
        index = 0
    station_id = str(geo.data[('Points', 'I')].iloc[index]['station_id'][0])
    station_data = buoy_dict[station_id]
    return hv.Scatter(station_data, 'timestamp', 'ATMP').opts(width=900, height=900)
    #return hv.Scatter([station_data['timestamp'].values.astype(float), station_data['ATMP'].values])
    
buoy = hv.DynamicMap(click_callback, streams=[tap_stream])

tap_stream.add_subscriber(click_callback)

layout = geo + buoy

app = pn.Column("# NOAA Realtime Dashboard", intro, layout, sizing_mode='stretch_both')
app.servable();
pn.serve(app, port=8080)

while True:
    pass
