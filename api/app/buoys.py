from fastapi import APIRouter, HTTPException
import logging as logger
import pandas as pd
import re
from .buoy import Buoy

STATIONS_URL = "https://www.ndbc.noaa.gov/data/stations/station_table.txt"
OWNERS_URL = "https://www.ndbc.noaa.gov/data/stations/station_owners.txt"

router = APIRouter()

@router.get('/')
async def get_buoys():
    buoys = pd.read_csv(STATIONS_URL, delimiter = '|').iloc[1:,:]
    buoys.columns = ['buoy_id', 'owner', 'ttype', 'hull', 'name', 'payload', 'location', 'timezone', 'forecast', 'note']
    buoys[['lat', 'lon']] = buoys.location.apply(lambda x: get_lat_lon(x))
    buoys.drop(['location', 'hull', 'payload', 'forecast', 'note'], axis=1, inplace=True)
    return buoys.fillna('').to_dict('records')

@router.get('/{buoy_id}')
async def get_buoy_by_id(buoy_id: str):
    buoys = pd.read_csv(STATIONS_URL, delimiter = '|').iloc[1:,:]
    buoys.columns = ['buoy_id', 'owner', 'ttype', 'hull', 'name', 'payload', 'location', 'timezone', 'forecast', 'note']
    buoy = buoys[buoys['buoy_id'] == buoy_id].fillna('').to_dict('records')
    if not buoy:
        raise HTTPException(status_code=404, detail="Buoy not found")
    buoy = buoy[0]
    buoy['lat'], buoy['lon'] = get_lat_lon(buoy['location'])
    del buoy['location']
    buoy['owner'] = get_owner(buoy['owner'])
    return buoy

@router.get('/{buoy_id}/realtime')
async def get_buoy_realtime_data_types(buoy_id: str):
    try:
        buoy = Buoy(buoy_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Buoy not found")
    return buoy.get_realtime_dtypes()

@router.get('/{buoy_id}/realtime/{dtype}')
async def get_buoy_realtime_data(buoy_id: str, dtype: str):
    try:
        buoy = Buoy(buoy_id)
    except ValueError:
        raise HTTPException(status_code=404, detail="Buoy not found")
    data = buoy.get_realtime(dtype).reset_index()
    return data.to_json(orient='records')


### Helper Methods

def get_owner(owner_code: str):
    try:
        owners = pd.read_csv(OWNERS_URL, delimiter="|", skiprows=1, index_col=0)
        owner = owners.loc["{:<3}".format(owner_code), :]
        return "{}, {}".format(owner[0].rstrip(), owner[1].rstrip())
    except:
        return owner_code

def get_lat_lon(location: str):
    lat_match = re.search(r'([0-9]{1,3}\.[0-9]{3}) ([NS])', location)
    lat = lat_match.group(1)
    if lat_match.group(2) == 'S':
        lat = '-' + lat
    lon_match = re.search(r'([0-9]{1,3}\.[0-9]{3}) ([WE])', location)
    lon = lon_match.group(1)
    if lon_match.group(2) == 'W':
        lon = '-' + lon
    return pd.Series([lat, lon])
    
