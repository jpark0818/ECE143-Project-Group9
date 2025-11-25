import pandas as pd
import matplotlib.pyplot as plt
import numpy as np
import os
from scipy.stats import pearsonr

def load_wastewater(path):
    """
    Load wastewater data and prepare for merging.
    """
    df = pd.read_csv(path)
    df['Sample_Date'] = pd.to_datetime(df['Sample_Date'])
    df = df.set_index('Sample_Date')
    return df

def load_weather(path):
    """
    Load weather data and prepare for merging.
    """
    df = pd.read_csv(path)
    df['date'] = pd.to_datetime(df['date'])
    df = df.set_index('date')
    
    # Remove timezone and normalize to date only
    df.index = df.index.tz_localize(None)
    df.index = df.index.normalize()
    return df

def merge_and_correlate(wastewater_csv, weather_csv, location_name):
    """
    Merges wastewater and weather datasets and computes correlations 
    between viral gene copies and humidity.
    """
    wastewater_data = load_wastewater(wastewater_csv)
    weather_data = load_weather(weather_csv)
    
    # Merge on date index
    df = wastewater_data.join(weather_data, how='inner')
    
    # Extract relevant columns
    df = df[['Mean viral gene copies/L', 'avg_humidity_%']].dropna()
    
    if len(df) == 0:
        print(f"No matching data found for {location_name}")
        return None, None
    
    # Calculate correlation
    correlation, p_value = pearsonr(df['avg_humidity_%'], df['Mean viral gene copies/L'])
    
    print(f"\n{location_name} Correlation Results:")
    print(f"Humidity vs Viral Gene Copies/L: {correlation:.4f}")
    print(f"P-value: {p_value:.4f}")
    print(f"Number of data points: {len(df)}")
    print("=" * 50)
    
    return df, correlation

def plot_correlation(df, location_name, correlation):
    """
    Create scatter plot with humidity on x-axis and viral gene copies on y-axis.
    """
    if df is None or len(df) == 0:
        return
    
    plt.figure(figsize=(10, 6))
    plt.scatter(df['avg_humidity_%'], df['Mean viral gene copies/L'], 
                alpha=0.6, s=50, edgecolors='black', linewidth=0.5)
    
    # Add trend line
    z = np.polyfit(df['avg_humidity_%'], df['Mean viral gene copies/L'], 1)
    p = np.poly1d(z)
    plt.plot(df['avg_humidity_%'], p(df['avg_humidity_%']), 
             "r--", alpha=0.8, linewidth=2, label=f'Trend line (r={correlation:.3f})')
    
    plt.xlabel('Average Humidity (%)', fontsize=12, fontweight='bold')
    plt.ylabel('Mean Viral Gene Copies/L', fontsize=12, fontweight='bold')
    plt.title(f'{location_name}: Viral Gene Copies vs Humidity', fontsize=14, fontweight='bold')
    plt.grid(True, alpha=0.3)
    plt.legend()
    plt.tight_layout()
    
    # Save plot in the same directory as the script
    script_dir = os.path.dirname(os.path.abspath(__file__))
    filename = os.path.join(script_dir, f'{location_name.replace(" ", "_")}_humidity_correlation.png')
    plt.savefig(filename, dpi=300, bbox_inches='tight')
    print(f"Plot saved as: {filename}\n")
    plt.close()

def main():
    """
    Main function to process all three locations.
    """
    # Get the directory where this script is located
    script_dir = os.path.dirname(os.path.abspath(__file__))
    project_dir = os.path.join(script_dir, '..', 'ECE143-Project-Group9')
    
    # Define file paths
    locations = {
        'Encina': {
            'wastewater': os.path.join(project_dir, 'Wastewater_Data', 'Encina_sewage_qPCR_Modified.csv'),
            'weather': os.path.join(project_dir, 'weather_Encina.csv')
        },
        'Point Loma': {
            'wastewater': os.path.join(project_dir, 'Wastewater_Data', 'PointLoma_sewage_qPCR_Modified.csv'),
            'weather': os.path.join(project_dir, 'weather_Point Loma.csv')
        },
        'South Bay': {
            'wastewater': os.path.join(project_dir, 'Wastewater_Data', 'SouthBay_sewage_qPCR_Modified.csv'),
            'weather': os.path.join(project_dir, 'weather_South Bay.csv')
        }
    }
    
    correlations = {}
    
    # Process each location
    for location_name, paths in locations.items():
        df, corr = merge_and_correlate(paths['wastewater'], paths['weather'], location_name)
        if df is not None:
            plot_correlation(df, location_name, corr)
            correlations[location_name] = corr
    
    # Print summary
    print("\n" + "=" * 50)
    print("SUMMARY OF CORRELATIONS:")
    print("=" * 50)
    for location, corr in correlations.items():
        print(f"{location:15s}: {corr:7.4f}")
    print("=" * 50)

if __name__ == "__main__":
    main()

