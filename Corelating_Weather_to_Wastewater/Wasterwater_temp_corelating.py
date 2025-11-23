import pandas as pd

def load_wastewater(path):

    df = pd.read_csv(path)
    df['Sample_Date'] = pd.to_datetime(df['Sample_Date'])
    df = df.set_index('Sample_Date')
    return df

def load_weather(path):
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')

    df.index = df.index.tz_localize(None)   
    df.index = df.index.normalize()         
    return df


def merge_and_correlate(wastewater_csv, weather_csv):
    '''
    Merges wastewater and weather datasets and computes correlations between Z-scores and temperature.
    '''
    wastewater_data = load_wastewater(wastewater_csv)
    weather_data = load_weather(weather_csv)

    df = wastewater_data.join(weather_data, how='inner')

    df['avg_temp'] = (df['min_temp_c'] + df['max_temp_c']) / 2

    corr_min = df['zscore'].corr(df['min_temp_c'])
    corr_max = df['zscore'].corr(df['max_temp_c'])
    corr_avg = df['zscore'].corr(df['avg_temp'])

    print("CORRELATION RESULTS:")
    print(f"Z-score vs MIN temp: {corr_min:.4f}")
    print(f"Z-score vs MAX temp: {corr_max:.4f}")
    print(f"Z-score vs AVG temp: {corr_avg:.4f}")
    print("================================\n")

    return df

def weekly_correlation(wastewater_data, weather_data, window=7):
    """
    Computes 7-day rolling-average correlations between wastewater z-scores
    and weather
    """

    df = wastewater_data.join(weather_data, how="inner")

    df["avg_temp"] = (df["min_temp_c"] + df["max_temp_c"]) / 2

    df = df.sort_index()

    df["z_week"]   = df["zscore"].rolling(window=window).mean()
    df["min_week"] = df["min_temp_c"].rolling(window=window).mean()
    df["max_week"] = df["max_temp_c"].rolling(window=window).mean()
    df["avg_week"] = df["avg_temp"].rolling(window=window).mean()

    df2 = df.dropna()

    corr_min = df2["z_week"].corr(df2["min_week"])
    corr_max = df2["z_week"].corr(df2["max_week"])
    corr_avg = df2["z_week"].corr(df2["avg_week"])

    print("WEEKLY (7-day averaged) CORRELATION:")
    print(f"Weekly Z-score vs Weekly MIN temp: {corr_min:.4f}")
    print(f"Weekly Z-score vs Weekly MAX temp: {corr_max:.4f}")
    print(f"Weekly Z-score vs Weekly AVG temp: {corr_avg:.4f}")
    print("================================================\n")

    return df2


print("Encina Corelation Results:")
merge_and_correlate("Wastewater_Data/Encina_sewage_qPCR_Modified.csv","weather_Encina.csv")

print("Encina Weekly Results:")
weekly_correlation(
    load_wastewater("Wastewater_Data/Encina_sewage_qPCR_Modified.csv"),
    load_weather("weather_Encina.csv")
)


print("PointLoma Corelation Results:")
merge_and_correlate("Wastewater_Data/PointLoma_sewage_qPCR_Modified.csv","weather_Point Loma.csv")

print("PointLoma Weekly Results:")
weekly_correlation(
    load_wastewater("Wastewater_Data/PointLoma_sewage_qPCR_Modified.csv"),
    load_weather("weather_Point Loma.csv")
)


print("South Bay Corelation Results:")
merge_and_correlate("Wastewater_Data/SouthBay_sewage_qPCR_Modified.csv","weather_South Bay.csv")

print("PointLoma Weekly Results:")
weekly_correlation(
    load_wastewater("Wastewater_Data/SouthBay_sewage_qPCR_Modified.csv"),
    load_weather("weather_South Bay.csv")
)
