import openmeteo_requests
import pandas as pd
import requests_cache
from retry_requests import retry
from datetime import date 

'''Parse and store weather data for 3 San Diego regions'''

# --- Setup cached+retry session ---
cache_session = requests_cache.CachedSession('.cache', expire_after=3600)
retry_session = retry(cache_session, retries=5, backoff_factor=0.2)
openmeteo = openmeteo_requests.Client(session=retry_session)

url = "https://archive-api.open-meteo.com/v1/archive"


LOCATIONS = {
    "Encina":      (32.69, -117.1611),
    "Point Loma":  (32.697, -117.236),
    "South Bay":   (32.592, -117.087)
}

START_DATE = date(2022, 1, 1)
END_DATE   = date(2025, 10, 31)


DAILY_VARS = ["relative_humidity_2m_mean",
              "temperature_2m_max",
              "temperature_2m_min",
              "wind_speed_10m_mean"]


def fetch_location_data(name, lat, lon):
    """Fetch daily weather data for a single location"""

    params = {
        "latitude": lat,
        "longitude": lon,
        "start_date": START_DATE.isoformat(),
        "end_date": END_DATE.isoformat(),
        "daily": DAILY_VARS,
        "timezone": "America/Los_Angeles"
    }

    responses = openmeteo.weather_api(url, params=params)
    response = responses[0]

    daily = response.Daily()

    humidity = daily.Variables(0).ValuesAsNumpy()
    temp_max = daily.Variables(1).ValuesAsNumpy()
    temp_min = daily.Variables(2).ValuesAsNumpy()
    wind_avg = daily.Variables(3).ValuesAsNumpy()

    dates = pd.date_range(
        start=pd.to_datetime(daily.Time(), unit="s", utc=True),
        end=pd.to_datetime(daily.TimeEnd(), unit="s", utc=True),
        freq=pd.Timedelta(seconds=daily.Interval()),
        inclusive="left"
    )

    df = pd.DataFrame({
        "date": dates,
        "max_temp_c": temp_max,
        "min_temp_c": temp_min,
        "avg_humidity_%": humidity,
        "avg_wind_speed_m_s": wind_avg
    })

    return df


all_data = {}

for name, (lat, lon) in LOCATIONS.items():
    df = fetch_location_data(name, lat, lon)
    all_data[name] = df

    filename = f"weather_{name}.csv"
    df.to_csv(filename, index=False)
    print(f"Saved CSV: {filename}")

