import openmeteo_requests

import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import date 

'''Parse and store weather data for san diego'''

cache_session = requests_cache.CachedSession('.cache', expire_after = 3600)
retry_session = retry(cache_session, retries = 5, backoff_factor = 0.2)
openmeteo = openmeteo_requests.Client(session = retry_session)

url = "https://archive-api.open-meteo.com/v1/archive"


SAN_DIEGO_LATITUDE = 32.7157
SAN_DIEGO_LONGITUDE = -117.1611


START_DATE = date(2024, 10, 1)
END_DATE = date(2024, 10, 31)

params = {
	"latitude": SAN_DIEGO_LATITUDE,
	"longitude": SAN_DIEGO_LONGITUDE,
    "start_date": START_DATE.isoformat(),
    "end_date": END_DATE.isoformat(),
	"daily": ["temperature_2m_max", "temperature_2m_min", "rain_sum"],
    "timezone": "America/Los_Angeles"
}

responses = openmeteo.weather_api(url, params=params)

response = responses[0]
print(f"Coordinates (San Diego): {response.Latitude()}°N {response.Longitude()}°E")
print(f"Elevation: {response.Elevation()} m asl")

daily = response.Daily()

daily_temperature_2m_max = daily.Variables(0).ValuesAsNumpy()
daily_temperature_2m_min = daily.Variables(1).ValuesAsNumpy()
daily_rain_sum = daily.Variables(2).ValuesAsNumpy()

daily_data = {"date": pd.date_range(
	start = pd.to_datetime(daily.Time(), unit = "s", utc = True),
	end =  pd.to_datetime(daily.TimeEnd(), unit = "s", utc = True),
	freq = pd.Timedelta(seconds = daily.Interval()),
	inclusive = "left"
)}

daily_data["max_temp_c"] = daily_temperature_2m_max
daily_data["min_temp_c"] = daily_temperature_2m_min
daily_data["rain_mm"] = daily_rain_sum

daily_dataframe = pd.DataFrame(data = daily_data)
print("\nDaily Historical Data for San Diego County (Representative Location)\n", daily_dataframe)