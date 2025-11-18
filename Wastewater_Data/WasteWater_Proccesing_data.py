import pandas as pd
import matplotlib.pyplot as plt

def load_wastewater(csv_path):
    df = pd.read_csv(csv_path)
    df['Sample_Date'] = pd.to_datetime(df['Sample_Date'])
    df = df.sort_values('Sample_Date')
    df = df.set_index('Sample_Date')
    return df


def interpolate_daily(df):
    df_daily = df.resample('D').interpolate(method='linear')
    return df_daily

def smooth_signal(df_daily):
    df_daily['smoothed'] = df_daily.iloc[:, 0].rolling(7, center=True).mean()
    return df_daily

def normalize(df_daily):
    col = df_daily.columns[0]
    df_daily['zscore'] = (df_daily[col] - df_daily[col].mean()) / df_daily[col].std()
    return df_daily

def process_wastewater(csv_path):
    """
    Steps performed:
    - Load CSV 
    - Convert to daily data 
    - Smooth data with 7-day rolling average
    - Add z-score 
    """
    df = load_wastewater(csv_path)
    df_daily = interpolate_daily(df)
    df_daily = smooth_signal(df_daily)
    df_daily = normalize(df_daily)
    return df_daily

def plot_data(df_daily):
    plt.figure(figsize=(12, 5))
    plt.plot(df_daily.index, df_daily.iloc[:, 0], label='Original')
    plt.plot(df_daily.index, df_daily['smoothed'], label='7-Day Smoothed')
    plt.title('Wastewater COVID Concentration Over Time')
    plt.xlabel('Date')
    plt.ylabel('Viral gene copies / L')
    plt.legend()
    plt.tight_layout()
    plt.show()


if __name__ == "__main__":
    file_path = "SouthBay_sewage_qPCR.csv"
    df_processed = process_wastewater(file_path)

    df_processed.to_csv("SouthBay_sewage_qPCR_Modified.csv")

    print(df_processed)
    plot_data(df_processed)
