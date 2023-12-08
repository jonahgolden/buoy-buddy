from fastapi import APIRouter, HTTPException
import asyncio
import requests as req
from xml.dom import minidom
import re

# from . import helpers

api_url = 'https://sdf.ndbc.noaa.gov/sos/server.php'
station_code = 'urn:ioos:station:wmo:'

router = APIRouter()

@router.get('/')
async def get_stations():
    stations = await get_stations()
    return stations

@router.get('/{id}')
async def get_stations(id: str):
    description = await get_station_description(id)
    return description

async def get_properties():
    properties = []

    loop = asyncio.get_event_loop()
    future_resp = loop.run_in_executor(None, req.get, f'{api_url}?request=GetCapabilities&service=SOS&sections=ServiceIdentification,OperationsMetadata')
    resp = await future_resp

    root = minidom.parseString(resp.text)
    keyword_els = root.getElementsByTagName('ows:Keyword')
    param_names = [k.firstChild.data for k in keyword_els]

    param_els = root.getElementsByTagName('ows:Parameter')
    for p_el in param_els:
        if p_el.getAttribute('name') == 'observedProperty':
            val_els = p_el.getElementsByTagName('ows:Value')
            for i in range(len(val_els)):
                properties.append({
                    'id': val_els[i].firstChild.data,
                    'name': param_names[-len(val_els) + i]
                })
            break
    
    return properties

async def get_stations():
    all_stations = []

    loop = asyncio.get_event_loop()
    future_resp = loop.run_in_executor(None, req.get, f'{api_url}?request=GetCapabilities&service=SOS&sections=Contents')
    resp = await future_resp

    root = minidom.parseString(resp.text)
    offering_els = root.getElementsByTagName('sos:ObservationOffering')
    for o in offering_els[1:]:
        # name = o.getAttribute('gml:id')
        name = o.getElementsByTagName('sos:procedure')[0].getAttribute('xlink:href').replace(station_code, '')

        desc_el = o.getElementsByTagName('gml:description')[0].firstChild

        bounded_by = o.getElementsByTagName('gml:boundedBy')
        lower_el = o.getElementsByTagName('gml:lowerCorner')[0].firstChild
        upper_el = o.getElementsByTagName('gml:upperCorner')[0].firstChild
        lower_corner = lower_el.data.split(' ') if lower_el else None
        upper_corner = upper_el.data.split(' ') if upper_el else None
        if lower_corner != upper_corner:
            print(f'Lower != Upper : {name}')
        coords = lower_corner if lower_corner else upper_corner if upper_corner else [None, None]
        lat, lon = coords
        
        time_el = o.getElementsByTagName('sos:time')
        begin = o.getElementsByTagName('gml:beginPosition')[0].firstChild
        end = o.getElementsByTagName('gml:endPosition')[0].firstChild

        property_els = o.getElementsByTagName('sos:observedProperty')
        properties = [p_el.getAttribute('xlink:href').split('/')[-1] for p_el in property_els]

        # feauture_els = o.getElementsByTagName('sos:featureOfInterest')
        # features = [f_el.getAttribute('xlink:href') for f_el in feature_els]

        all_stations.append({
            'name': name, #.replace('station-', '') if name else None,
            'desc': desc_el.data if desc_el else None,
            'latitude': float(lat),
            'longitude': float(lon),
            'begin_time': begin.data if begin else None,
            'end_time': end.data if end else None,
            'properties': properties,
        })
    return all_stations

all_sensors = []
all_attributes = []

async def get_station_description(name):
    desc = {
        'sensors': []
    }

    loop = asyncio.get_event_loop()
    future_desc = loop.run_in_executor(None, req.get, f'{api_url}?request=DescribeSensor&service=SOS&version=1.0.0&outputformat=text/xml;subtype=%22sensorML/1.0.1%22&procedure={station_code}{name}')
    description = await future_desc

    root = minidom.parseString(description.text)

    classifiers = root.getElementsByTagName('sml:classifier')
    for c in classifiers:
        c_name = get_snake_case(c.getAttribute('name'))
        if c_name not in all_attributes:
            all_attributes.append(c_name)
        val = c.getElementsByTagName('sml:value')[0].firstChild
        if val:
            desc[c_name] = val.data
    
    sensors = root.getElementsByTagName('sml:component')
    for s in sensors:
        s_name = s.getAttribute('name').replace(f'urn:ioos:sensor:wmo:{name}::', '')
        if s_name not in all_sensors:
            all_sensors.append(s_name)

        desc['sensors'].append(s_name)
    
    return desc

async def get_sensor_description(station_name, sensor_name):
    return None

def get_snake_case(txt):
    return re.sub('([A-Z]+)', r'_\1', txt).lower()

