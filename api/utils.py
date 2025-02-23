import json
import requests
import pandas as pd
# import ray
import os
from sqlalchemy.orm import sessionmaker, Session
from sqlalchemy import (Table, 
                        Column, 
                        MetaData, 
                        create_engine, 
                        Float, 
                        TIMESTAMP,
                        String,
                        text)
                        

COLs= ['latitude', 'longitude', 'city_name', 'time', 
       'temperature_2m','relative_humidity_2m', 'wind_speed_10m']

def cities_from_geojson(filepath):
    with open(filepath, 'r') as f:
        json_obj = json.load(f)
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

def get_SessionLocal():
    # Read from environment variables, or use default values
    CCDB_USER = os.getenv('CCDB_USER', 'postgres')
    CCDB_PASS = os.getenv('CCDB_PASS', 'mysecretpassword')
    HOST = os.getenv('HOST', 'my-postgres')
    PORT = os.getenv('PORT', '5432')
    DB = os.getenv('DB', 'postgres')

    # Create the database engine
    engine = create_engine(
    f'postgresql://{CCDB_USER}:{CCDB_PASS}@{HOST}:{PORT}/{DB}',
    pool_size=10,  # Max number of connections to keep in the pool
    max_overflow=20,  # Allow up to 20 extra connections
    pool_timeout=30,  # Wait up to 30 seconds for a connection
    pool_recycle=1800  # Recycle connections every 30 minutes (to avoid stale connections)
    )
    SessionLocal = sessionmaker(bind=engine, autoflush=False, autocommit=False)
    
    return SessionLocal()

def get_db():
    """Dependency to get database session"""
    db = get_SessionLocal()
    try:
        yield db  # Give the session to the request
    finally:
        db.close()  # Close session after request



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
                 year:str,
                 db: Session):
    

    create_table(year,db.bind)
    return df.to_sql(name=f'climate_{year}',schema=schema,con= db.bind, if_exists='append', index=False)


def read_from_DB(year:str,
                 city:str,
                 db:Session):


    # Read table into DataFrame
    sql = text(f"SELECT * FROM weather.climate_{year} WHERE city_name = :city")
    df = pd.read_sql(sql=sql, con=db.bind,params={"year": year, "city": city.capitalize()})
    df.drop('city_name',axis=1,inplace=True)
    return df.to_dict()