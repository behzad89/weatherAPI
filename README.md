# City Weather API
🌍 Welcome to the City Weather API!

This API provides weather data for cities around the world allowing to retrieve essential daily weather details like temperature, humidity, wind speed, and more for any city. The data is prepared using hourly historical  weather information from [**ERA5**]("https://cds.climate.copernicus.eu/datasets/reanalysis-era5-single-levels?tab=overview") model provided by [Open-Meteo](https://open-meteo.com/").


## Overview
The City Weather API allows you to query daily weather data, for any city in the world. This API provides essential parameters like temperature, humidity, wind speed, and more, enabling you to build comprehensive weather applications.

# Data Explanation
The API provides the following key data points:

### 1. Latitude and Longitude
Latitude: The north-south position of a city (ranging from -90° to +90°).
Longitude: The east-west position of a city (ranging from -180° to +180°).
Together, these coordinates uniquely identify the location of a city.
### 2. Date
Time refers to the specific date when the weather data is recorded.
### 3. Temperature_2m
Temperature at 2 meters is the air temperature measured 2 meters above the ground. This is the standard height used in weather reporting and is typically given in Celsius (°C).
### 4. Relative Humidity_2m
Relative Humidity at 2 meters is the percentage of water vapor in the air, measured at 2 meters above the ground. It indicates how much moisture is in the air compared to the maximum moisture the air could hold at that temperature. It is expressed as a percentage (0-100%).
### 5. Wind Speed_10m
Wind Speed at 10 meters measures the wind speed 10 meters above the ground. This standard height ensures that the wind data is not influenced by ground-level obstructions like buildings or trees. Wind speed is typically measured in km/h (kilometers per hour).

## Summary of Data Flow:  
1. **Extract:** Weather data is pulled from Open-Meteo.  
2. **Transform:** The data is processed using Oracle.  
3. **Load:** The processed data is stored in an Aiven-managed PostgreSQL database.  
4. **Orchestration:** Prefect manages workflow automation.  
5. **Deployment:** CI/CD via GitHub, deployment on Vercel.  
6. **API Interaction:** Users access the weather data via a FastAPI-based service hosted on Vercel.  

This setup ensures efficient weather data retrieval, transformation, and API-based access for users.
![Weather API Architecture](WeatherAPI_white.png)
