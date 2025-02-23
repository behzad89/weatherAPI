import json
import requests
import pandas as pd
# import ray
import os
from sqlalchemy import (Table, 
                        Column, 
                        MetaData, 
                        create_engine, 
                        Float, 
                        TIMESTAMP,
                        String)
                        

COLs= ['latitude', 'longitude', 'city_name', 'time', 
       'temperature_2m','relative_humidity_2m', 'wind_speed_10m']

def cities_from_geojson(json_obj):
        return json_obj['features']

# @ray.remote
def get_weather_data(latitude:float,
                     longitude:float,
                     start_date:str,
                     end_date:str):

    url = (
        "https://archive-api.open-meteo.com/v1/era5?"
        f"latitude={latitude}&longitude={longitude}&"
        f"start_date={start_date}&end_date={end_date}&"
        "hourly=temperature_2m,relative_humidity_2m,wind_speed_10m"
        )
    
    # Get the weather data
    response = requests.get(url)
    data = response.json()

    return data

# @ray.remote
def convert_to_daily_df(json_data,city_name,cols=COLs):
    # Extract hourly data
    hourly_data = json_data['hourly']
    df = pd.DataFrame(hourly_data)
    df['time'] = pd.to_datetime(df['time'])
    df.set_index(['time'],inplace=True)
    df = df.resample('D').mean().reset_index()
    # Add latitude and longitude to each row
    df['latitude'] = json_data['latitude']
    df['longitude'] = json_data['longitude']
    df['city_name'] = city_name

    # Reorder columns as desired
    df = df[cols]
    return df

def db_config():
    # Read from environment variables, or use default values
    CCDB_USER = os.getenv('CCDB_USER', 'postgres')
    CCDB_PASS = os.getenv('CCDB_PASS', 'mysecretpassword')
    HOST = os.getenv('HOST', 'my-postgres')
    PORT = os.getenv('PORT', '5432')
    DB = os.getenv('DB', 'postgres')

    # Create the database engine
    engine = create_engine(f'postgresql://{CCDB_USER}:{CCDB_PASS}@{HOST}:{PORT}/{DB}')
    
    return engine

def create_table(year:int,
                 conn:str):
    metadata = MetaData()
    climate_table = Table(
        f'climate_{year}', metadata,
        Column('time', TIMESTAMP),
        Column('latitude', Float),
        Column('longitude', Float),
        Column('city_name', String),
        Column('temperature_2m', Float),
        Column('relative_humidity_2m', Float),
        Column('wind_speed_10m', Float),
        schema='weather' 
    )
    metadata.create_all(conn)

# @ray.remote
def ingest_to_db(df:pd.DataFrame,
                 schema:str,
                 year=str):
    

    engine = db_config()
    create_table(year,engine)
    return df.to_sql(name=f'climate_{year}',schema=schema,con= engine, if_exists='append', index=False)


def read_from_DB(year:str,
                 city:str):

    engine = db_config()

    # Read table into DataFrame
    sql = f"select * from weather.climate_{year} where climate_{year}.city_name  = '{city.capitalize()}'"
    df = pd.read_sql(sql=sql, con=engine)
    return df.to_dict()


geojson = {"type":"FeatureCollection","features":[{"type":"Feature","properties":{"city_name":"London"},"geometry":{"coordinates":[-0.12163434564180875,51.5016303931763],"type":"Point"},"id":0},{"type":"Feature","properties":{"city_name":"Cardiff"},"geometry":{"coordinates":[-3.170676094902177,51.48466197944012],"type":"Point"},"id":1},{"type":"Feature","properties":{"city_name":"Bristol"},"geometry":{"coordinates":[-2.5841158826883373,51.45489286558609],"type":"Point"},"id":2},{"type":"Feature","properties":{"city_name":"Birmingham"},"geometry":{"coordinates":[-1.874632115742287,52.47611937450699],"type":"Point"},"id":3},{"type":"Feature","properties":{"city_name":"Nottingham"},"geometry":{"coordinates":[-1.143188775695819,52.95230260148071],"type":"Point"},"id":4},{"type":"Feature","properties":{"city_name":"Sheffield"},"geometry":{"coordinates":[-1.4523691070393454,53.38615494433887],"type":"Point"},"id":5},{"type":"Feature","properties":{"city_name":"Wakefield"},"geometry":{"coordinates":[-1.4875707699151235,53.68282203401111],"type":"Point"},"id":6},{"type":"Feature","properties":{"city_name":"Manchester"},"geometry":{"coordinates":[-2.2535596807031197,53.46992655854564],"type":"Point"},"id":7},{"type":"Feature","properties":{"city_name":"Warrington"},"geometry":{"coordinates":[-2.584021302736801,53.38207241607904],"type":"Point"},"id":8},{"type":"Feature","properties":{"city_name":"Chester"},"geometry":{"coordinates":[-2.8722637087616363,53.180396651669014],"type":"Point"},"id":9}]}
