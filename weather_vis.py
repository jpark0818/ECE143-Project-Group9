import argparse
import os
from datetime import datetime, timedelta
from typing import Dict, Tuple, Optional

import pandas as pd
import geopandas as gpd
import matplotlib.pyplot as plt
import osmnx as ox
import requests


def fetch_weather(lat: float, lon: float, start: Optional[str] = None, end: Optional[str] = None,
                  session: Optional[requests.Session] = None) -> pd.DataFrame:
    """Fetch hourly weather data from Open-Meteo for a single location.

    Args:
        lat: Latitude of location (decimal degrees).
        lon: Longitude of location (decimal degrees).
        start: Start date in 'YYYY-MM-DD' format. Defaults to 2 days ago (UTC).
        end: End date in 'YYYY-MM-DD' format. Defaults to today (UTC).
        session: Optional `requests.Session` to use for HTTP requests.

    Returns:
        DataFrame with columns ['time', 'temperature', 'humidity', 'wind_speed'].

    Raises:
        AssertionError: if inputs are out of range or API response missing expected keys.
    """

    assert isinstance(lat, (int, float)) and -90 <= lat <= 90, "lat must be numeric between -90 and 90"
    assert isinstance(lon, (int, float)) and -180 <= lon <= 180, "lon must be numeric between -180 and 180"

    if start is None:
        start = (datetime.utcnow() - timedelta(days=2)).strftime("%Y-%m-%d")
    if end is None:
        end = datetime.utcnow().strftime("%Y-%m-%d")

    url = "https://api.open-meteo.com/v1/forecast"
    # Open-Meteo expects hourly variables as comma-separated string
    params = {
        "latitude": lat,
        "longitude": lon,
        "hourly": "temperature_2m,relativehumidity_2m,windspeed_10m",
        "start_date": start,
        "end_date": end,
        "timezone": "UTC",
    }

    sess = session or requests.Session()
    resp = sess.get(url, params=params, timeout=30)
    resp.raise_for_status()
    data = resp.json()

    assert "hourly" in data, "Unexpected API response: missing 'hourly'"
    hourly = data["hourly"]

    # Accept multiple possible key names with fallbacks
    time_key = "time"
    temp_key = "temperature_2m"
    hum_key = "relativehumidity_2m"
    wind_key = "windspeed_10m"

    for key in (time_key, temp_key, hum_key, wind_key):
        assert key in hourly, f"API hourly response missing '{key}'"

    times = pd.to_datetime(hourly[time_key])
    temp = pd.Series(hourly[temp_key])
    humidity = pd.Series(hourly[hum_key])
    wind = pd.Series(hourly[wind_key])

    min_len = min(len(times), len(temp), len(humidity), len(wind))
    df = pd.DataFrame({
        "time": times[:min_len],
        "temperature": temp.iloc[:min_len].astype(float).values,
        "humidity": humidity.iloc[:min_len].astype(float).values,
        "wind_speed": wind.iloc[:min_len].astype(float).values,
    })

    return df


def plot_spatial(combined: pd.DataFrame, gdf: gpd.GeoDataFrame, county: gpd.GeoDataFrame,
                 timestamp: pd.Timestamp, variable: str, output_path: Optional[str] = None,
                 show: bool = True) -> None:
    """Plot spatial distribution of a weather variable at a given timestamp.

    Args:
        combined: Combined DataFrame with weather for all locations (must include 'location' and 'time').
        gdf: GeoDataFrame with 'location' and geometry columns.
        county: GeoDataFrame of the county polygon(s).
        timestamp: Timestamp to visualize (must exist in combined['time']).
        variable: One of 'temperature', 'humidity', 'wind_speed'.
        output_path: Optional path to save the figure (PNG).
        show: If True, call `plt.show()` to display interactively.

    Raises:
        AssertionError: if variable is invalid or timestamp missing.
    """

    allowed = {"temperature", "humidity", "wind_speed"}
    assert variable in allowed, f"variable must be one of {allowed}"
    assert pd.to_datetime(timestamp) in list(pd.to_datetime(combined["time"])), "timestamp not found in combined data"

    snapshot = combined[combined["time"] == pd.to_datetime(timestamp)]
    spatial = gdf.merge(snapshot, on="location", how="inner")

    fig, ax = plt.subplots(figsize=(9, 9))
    county.boundary.plot(ax=ax, edgecolor="black", linewidth=1.2)
    spatial.plot(column=variable, ax=ax, cmap="coolwarm", legend=True, markersize=120)

    for _, row in spatial.iterrows():
        ax.text(row.geometry.x + 0.02, row.geometry.y + 0.02, row["location"], fontsize=10)

    ax.set_title(f"{variable.title()} at {pd.to_datetime(timestamp)}")
    if output_path:
        fig.savefig(output_path, dpi=150, bbox_inches="tight")
    if show:
        plt.show()
    plt.close(fig)


def main(args=None):
    """Main entrypoint for script.

    Fetches weather for a small set of locations in San Diego County and
    generates a static map for the most recent timestamp.
    """

    parser = argparse.ArgumentParser(description="Fetch and visualize weather for a few stations")
    parser.add_argument("--start", help="Start date YYYY-MM-DD", default=None)
    parser.add_argument("--end", help="End date YYYY-MM-DD", default=None)
    parser.add_argument("--out", help="Output directory to save figures", default="outputs")
    parser.add_argument("--show", help="Show plots interactively", action="store_true")
    parsed = parser.parse_args(args=args)

    os.makedirs(parsed.out, exist_ok=True)

    locations: Dict[str, Tuple[float, float]] = {
        "Encina": (33.1387, -117.3258),
        "Point Loma": (32.6840, -117.2400),
        "South Bay": (32.5881, -117.0330),
    }

    # build GeoDataFrame of station points (lon, lat order for points_from_xy)
    lons = [lon for _, lon in locations.values()]
    lats = [lat for lat, _ in locations.values()]
    gdf = gpd.GeoDataFrame({"location": list(locations.keys())},
                           geometry=gpd.points_from_xy(lons, lats),
                           crs="EPSG:4326")

    weather_data = []
    sess = requests.Session()
    for loc, (lat, lon) in locations.items():
        df = fetch_weather(lat=lat, lon=lon, start=parsed.start, end=parsed.end, session=sess)
        df["location"] = loc
        weather_data.append(df)

    assert weather_data, "No weather data fetched"
    combined = pd.concat(weather_data, ignore_index=True)
    combined["time"] = pd.to_datetime(combined["time"])  # normalize

    county = ox.geocode_to_gdf("San Diego County, California, USA").to_crs(epsg=4326)

    # Plot county + station markers
    fig, ax = plt.subplots(figsize=(9, 9))
    county.boundary.plot(ax=ax, edgecolor="black", linewidth=1.5)
    gdf.plot(ax=ax, color="red", markersize=120)
    for _, row in gdf.iterrows():
        ax.text(row.geometry.x + 0.02, row.geometry.y + 0.02, row["location"], fontsize=11)
    ax.set_title("San Diego County with Weather Stations")
    map_path = os.path.join(parsed.out, "san_diego_stations.png")
    fig.savefig(map_path, dpi=150, bbox_inches="tight")
    if parsed.show:
        plt.show()
    plt.close(fig)

    # pick most recent timestamp and plot each variable
    latest = combined["time"].max()
    for var in ["temperature", "humidity", "wind_speed"]:
        out_file = os.path.join(parsed.out, f"{var}_{latest.strftime('%Y%m%dT%H%M')}.png")
        plot_spatial(combined=combined, gdf=gdf, county=county, timestamp=latest, variable=var,
                     output_path=out_file, show=parsed.show)


if __name__ == "__main__":
    main()