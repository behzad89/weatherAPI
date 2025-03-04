from utils import (cities_from_geojson,
                   get_weather_data,
                   convert_to_daily_df,
                   ingest_to_db,
                   get_db)

from prefect import flow, task

@task
def city_weather_info(city: dict, start_date: str, end_date: str):
    city_name = city['properties']['city_name']
    longitude, latitude = city['geometry']['coordinates']

    json_data = get_weather_data(latitude, longitude, start_date, end_date)
    dataframe = convert_to_daily_df(json_data,city_name)

    return dataframe

@flow(name="ET", retries=3, retry_delay_seconds=5)
def extract_transform(cities, start_date, end_date):
    print("Extract the weather information of the cities")
    dfs = [city_weather_info.submit(city, start_date, end_date) for city in cities]
    return dfs

@task(name="Load_To_DB")
def load(df, year):
    print("Load the dataframes to DB")
    db = next(get_db())
    ingest_to_db(df, 'weather', year,db)


@flow(name="ETL_Pipeline", retries=3, retry_delay_seconds=5)
def etl_pipeline(start_date: str, end_date: str):
    print("Reading cities from file...")
    cities_filepath = './data/cities.geojson'
    cities = cities_from_geojson(cities_filepath)

    year = start_date.split('-')[0]
    transformed_data = extract_transform(cities, start_date, end_date)

    for df in transformed_data:
        load.submit(df, year).result()

if __name__ == "__main__":
    
    # To deploy on Prefect
    etl_pipeline.serve(name="Weather_Data_To_DB",
                       description="Extract Weather data for cities from API and Load to DB")






    
