from fastapi import FastAPI, HTTPException, Path
from utils import cities_from_geojson,read_from_DB,geojson

cities = cities_from_geojson(geojson)

app = FastAPI()

@app.get('/')
def welcome():
    return {
        "message": "🌍 Welcome to the City Weather API!",
        "info": "Get weather details for cities across the world."
    }


@app.get('/instruction')
def instruction():
    return {"message": "The city name must be transmitted via a GET API request."}

@app.get("/city/city_list")
def get_cities():
    city_list = [city['properties']['city_name'] for city in cities]
    return {"city_list":city_list}

@app.get("/city/{city_name}/{year}")
def get_city(city_name: str = Path(..., title="City Name", description="Name of the city"),
    year: int = Path(..., gt=2019, lt=2025, title="Year", description="Year for weather details")):
    
    # Find the first matching city
    city_coords = next((c for c in cities if c['properties']["city_name"].lower() == city_name.lower()), None)

    # If city not found, return error response
    if not city_coords:
        raise HTTPException(status_code=404, detail="City not found in the list")
    
    else:
        return {"city": city_name, 
                "info": f"Weather details retrieved for {year}",
                "city_coords":city_coords,
                "daily_data":read_from_DB(year=year,city=city_name)
                }