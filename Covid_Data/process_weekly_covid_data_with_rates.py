from pathlib import Path
import pandas as pd


DATA_DIR = Path("Data_wrangling")
FY_LABEL = "2023-24"
FILES = {
    "Cases": "Covid_weekly_cases_2023_2024.csv",
    "Hospitalization": "Covid_weekly_hospitalizations_2023_2024.csv",
    "Deaths": "Covid_weekly_deaths_2023_2024.csv",
}
OUTPUT_PATH = DATA_DIR / f"COVID_weekly_processed_{FY_LABEL.replace('-', '_')}.csv"


def clean_count(col):
    return (
        col.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": None})
        .pipe(pd.to_numeric, errors="coerce")
    )


def clean_rate(col):
    """Clean rate column - similar to clean_count but keeps as float for rates."""
    return (
        col.astype(str)
        .str.replace(",", "", regex=False)
        .str.strip()
        .replace({"": None})
        .pipe(pd.to_numeric, errors="coerce")
    )


def parse_week_end(col):
    parsed = pd.to_datetime(col, errors="coerce")
    if parsed.isna().any():
        mask = parsed.isna()
        for fmt in ("%m-%d-%Y", "%m/%d/%Y", "%m-%d-%y", "%m/%d/%y"):
            parsed.loc[mask] = pd.to_datetime(col[mask], format=fmt, errors="coerce")
            mask = parsed.isna()
            if not mask.any():
                break
    if parsed.isna().any():
        bad = col[parsed.isna()].unique()
        raise ValueError(f"Unable to parse WkEndActual values: {bad[:5]}")
    return parsed


def prepare_metric(path, label):
    """Extract both count and rate columns from a metric CSV file."""
    df = pd.read_csv(path).copy()
    # Drop rows where WkEndActual is missing (empty rows at end of CSV)
    df = df.dropna(subset=["WkEndActual"])
    # Skip rows where WkEndActual is just whitespace/empty string
    df = df[df["WkEndActual"].astype(str).str.strip() != ""]
    
    df["WkEndActual_dt"] = parse_week_end(df["WkEndActual"])
    df["Wk Start"] = (df["WkEndActual_dt"] - pd.Timedelta(days=6)).dt.strftime("%Y-%m-%d")
    df["WkEndActual"] = df["WkEndActual_dt"].dt.strftime("%Y-%m-%d")
    
    # Extract both count and rate
    df[label] = clean_count(df["Count"])
    rate_col_name = f"{label}_Rate"
    df[rate_col_name] = clean_rate(df["Rate"])
    
    df = df[["Wk Start", "WkEndActual", label, rate_col_name]]
    df = df.drop_duplicates(subset=["Wk Start", "WkEndActual"], keep="first")
    return df


def interpolate_missing(df, columns):
    df = df.sort_values("WkEndActual").reset_index(drop=True)
    for col in columns:
        if col in df:
            df[col] = df[col].interpolate(method="linear", limit_direction="both")
    return df


def main():
    frames = []
    for metric, filename in FILES.items():
        path = DATA_DIR / filename
        if not path.exists():
            raise FileNotFoundError(f"Missing file: {path}")
        frames.append(prepare_metric(path, metric))

    merged = frames[0]
    for frame in frames[1:]:
        merged = merged.merge(frame, on=["Wk Start", "WkEndActual"], how="outer")

    # Prepare list of all columns to interpolate (both counts and rates)
    count_columns = list(FILES.keys())
    rate_columns = [f"{metric}_Rate" for metric in FILES.keys()]
    all_columns = count_columns + rate_columns
    
    merged = interpolate_missing(merged, all_columns)
    merged["FY"] = FY_LABEL
    
    # Rename rate columns to user-friendly names
    merged = merged.rename(columns={
        "Cases_Rate": "Case Rate (per 100K)",
        "Hospitalization_Rate": "Hospitalization Rate (per 100K)",
        "Deaths_Rate": "Death Rate (per Million)"
    })
    
    # Select final columns in desired order
    merged = merged[["FY", "Wk Start", "WkEndActual", 
                     "Cases", "Hospitalization", "Deaths",
                     "Case Rate (per 100K)", "Hospitalization Rate (per 100K)", "Death Rate (per Million)"]]
    
    # Convert counts to integers: cases, hospitalizations, and deaths are whole numbers
    # Interpolation may produce floats, so we round to nearest integer before saving
    for col in ["Cases", "Hospitalization", "Deaths"]:
        if col in merged.columns:
            merged[col] = merged[col].round().astype("Int64")  # Int64 supports NaN values
    
    # Rates stay as floats (they can have decimal values like 31.2 per 100K)
    
    merged = merged.sort_values("WkEndActual").reset_index(drop=True)
    merged.to_csv(OUTPUT_PATH, index=False)

    print(f"Processed data saved to {OUTPUT_PATH}")
    print(merged.head())


def merge_all_years():
    """Merge all processed CSV files from different years into one file."""
    pattern = "COVID_weekly_processed_*.csv"
    files = list(DATA_DIR.glob(pattern))
    
    if not files:
        print(f"No processed files found matching: {pattern}")
        return
    
    print(f"Found {len(files)} processed files:")
    for f in sorted(files):
        print(f"  - {f.name}")
    
    all_data = []
    for file_path in sorted(files):
        df = pd.read_csv(file_path)
        all_data.append(df)
        print(f"Loaded {file_path.name}: {len(df)} rows")
    
    # Concatenate all dataframes - keeps ALL rows, no dropping
    merged = pd.concat(all_data, ignore_index=True)
    
    # Convert counts to integers: ensure all count columns are whole numbers
    # (in case older processed files had floats before we added the conversion)
    for col in ["Cases", "Hospitalization", "Deaths"]:
        if col in merged.columns:
            merged[col] = merged[col].round().astype("Int64")
    
    # Rates stay as floats - no conversion needed
    
    # Sort by fiscal year and week end date
    merged = merged.sort_values(["FY", "WkEndActual"]).reset_index(drop=True)
    
    output_file = DATA_DIR / "COVID_weekly_processed_ALL_YEARS.csv"
    merged.to_csv(output_file, index=False)
    
    print(f"\nMerged file saved to: {output_file}")
    print(f"Total rows: {len(merged)}")
    print(f"Years: {sorted(merged['FY'].unique())}")
    print(f"Date range: {merged['WkEndActual'].min()} to {merged['WkEndActual'].max()}")


if __name__ == "__main__":
    main()
    merge_all_years()


