import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
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
    df["min_week_temp_c"] = df["min_temp_c"].rolling(window=window).mean()
    df["max_week_temp_c"] = df["max_temp_c"].rolling(window=window).mean()
    df["avg_week_temp_c"] = df["avg_temp"].rolling(window=window).mean()

    df2 = df.dropna()

    corr_min = df2["z_week"].corr(df2["min_week_temp_c"])
    corr_max = df2["z_week"].corr(df2["max_week_temp_c"])
    corr_avg = df2["z_week"].corr(df2["avg_week_temp_c"])

    print("WEEKLY (7-day averaged) CORRELATION:")
    print(f"Weekly Z-score vs Weekly MIN temp: {corr_min:.4f}")
    print(f"Weekly Z-score vs Weekly MAX temp: {corr_max:.4f}")
    print(f"Weekly Z-score vs Weekly AVG temp: {corr_avg:.4f}")
    print("================================================\n")

    return df2



def plot_correlation(df, x_column, y_column, location_name="location"):
    """
    Creates a scatter plot with regression line, labels it,
    labels correlation, and saves as PNG.
    """

    x = df[x_column].values
    y = df[y_column].values

    # Compute regression line
    slope, intercept = np.polyfit(x, y, 1)
    reg_line = slope * x + intercept

    # Compute correlation
    corr = df[x_column].corr(df[y_column])

    # Create plot
    plt.figure(figsize=(8, 6))

    # Scatter
    plt.scatter(x, y, alpha=0.7, label="Data Points")

    # Regression line (red)
    plt.plot(
        x,
        reg_line,
        linewidth=2,
        color='red',
        label=f"Regression Line (slope={slope:.3f})"
    )

    #X and Y labels
    pretty_x = x_column.replace("_", " ").title()
    pretty_y = "Zscore of Wastewater Data"

    plt.xlabel(pretty_x)
    plt.ylabel(pretty_y)
    plt.title(f"{pretty_y} vs {pretty_x} ({location_name})")

    # label correlation on graph
    plt.text(
        0.05, 0.95,
        f"Correlation r = {corr:.3f}",
        transform=plt.gca().transAxes,
        fontsize=12,
        verticalalignment='top'
    )

    plt.legend()

    script_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(
        script_dir,
        f"{location_name.replace(' ', '_')}_{y_column}_vs_{x_column}.png"
    )

    plt.savefig(filename, dpi=300, bbox_inches="tight")
    print(f"Plot saved as: {filename}\n")

    plt.close()

print("Encina Corelation Results:")
df=merge_and_correlate("Wastewater_Data/Encina_sewage_qPCR_Modified.csv","weather_Encina.csv")

plot_correlation(df, "avg_temp", "zscore", location_name="Encina")
plot_correlation(df, "max_temp_c", "zscore", location_name="Encina")

print("Encina Weekly Results:")
df2=weekly_correlation(
    load_wastewater("Wastewater_Data/Encina_sewage_qPCR_Modified.csv"),
    load_weather("weather_Encina.csv")
)
plot_correlation(df2, "max_week_temp_c", "z_week", location_name="Encina_Weekly")


print("PointLoma Corelation Results:")
df=merge_and_correlate("Wastewater_Data/PointLoma_sewage_qPCR_Modified.csv","weather_Point Loma.csv")
plot_correlation(df, "avg_temp", "zscore", location_name="PointLoma")
plot_correlation(df, "max_temp_c", "zscore", location_name="PointLoma")
plot_correlation(df, "max_temp_c", "zscore", location_name="PointLoma")

print("PointLoma Weekly Results:")
df2=weekly_correlation(
    load_wastewater("Wastewater_Data/PointLoma_sewage_qPCR_Modified.csv"),
    load_weather("weather_Point Loma.csv")
)
plot_correlation(df2, "max_week_temp_c", "z_week", location_name="PointLoma_Weekly")


print("South Bay Corelation Results:")
df=merge_and_correlate("Wastewater_Data/SouthBay_sewage_qPCR_Modified.csv","weather_South Bay.csv")

plot_correlation(df, "avg_temp", "zscore", location_name="South Bay")
plot_correlation(df, "max_temp_c", "zscore", location_name="South Bay")

print("South Bay Weekly Results:")
df2=weekly_correlation(
    load_wastewater("Wastewater_Data/SouthBay_sewage_qPCR_Modified.csv"),
    load_weather("weather_South Bay.csv")
)

plot_correlation(df2, "max_week_temp_c", "z_week", location_name="South Bay_Weekly")
