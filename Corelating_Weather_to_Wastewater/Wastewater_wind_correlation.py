import pandas as pd
import matplotlib.pyplot as plt

from scipy.stats import pearsonr, spearmanr, kendalltau

'''
Notes
- delay in dates will be taken into account from 0~21
- 2022/3/14 seems to be when the endemic phase starts, so it will be the start of the analysis
'''

def add_datetime_column(df):
    """
    Adds a timestamp column named 'datetime' to input dataframe, based on the 'Date' column
    """
    assert "Date" in df.columns, "'Date' column does not exist"

    df['datetime'] = pd.to_datetime(df['Date']).dt.strftime('%Y-%m-%d')
    return df


def correlation_test_pearson(x, y):
    """
    Conduct a Pearson Correlation test on the two input datasets.
    Returns a correlation coefficient.

    Args:
        x: dataset 1
        y: dataset 2
    Returns:
        out: Pearson correlation coefficient
    """
    pass

def correlation_test_kendall(x, y):
    pass

def correlation_test_spearman(x, y):
    pass


def main():
    path_point_loma_csv = "/workspace/projects/Schoolwork/ECE 143 Programming for Data Science/team_project/ECE143-Project-Group9/Corelating_Weather_to_Wastewater/wind_to_wastewater/wind_to_wastewater_Point_loma.csv"
    path_encina_csv = "/workspace/projects/Schoolwork/ECE 143 Programming for Data Science/team_project/ECE143-Project-Group9/Corelating_Weather_to_Wastewater/wind_to_wastewater/wind_to_wastewater_Encina.csv"
    path_south_bay = "/workspace/projects/Schoolwork/ECE 143 Programming for Data Science/team_project/ECE143-Project-Group9/Corelating_Weather_to_Wastewater/wind_to_wastewater/wind_to_wastewater_South_Bay.csv"
    path_output = "/workspace/projects/Schoolwork/ECE 143 Programming for Data Science/team_project/ECE143-Project-Group9/Corelating_Weather_to_Wastewater/wind_to_wastewater/Correlation_output.xlsx"

    # Read data
    df_point_loma = pd.read_csv(path_point_loma_csv)
    df_encina = pd.read_csv(path_encina_csv)
    df_south_bay = pd.read_csv(path_south_bay)

    # Format output dataframes
    writer = pd.ExcelWriter(path_output, engine='xlsxwriter')
    df_result_point_loma = pd.DataFrame(index=[i for i in range(22)], columns=["Date Delay", "Pearson Coefficient", "Pearson p-value", "Spearman Coefficient", "Spearman p-value", "Kendall Coefficient", "Kendall p-value"])
    df_result_encina = pd.DataFrame(index=[i for i in range(22)], columns=["Date Delay", "Pearson Coefficient", "Pearson p-value", "Spearman Coefficient", "Spearman p-value", "Kendall Coefficient", "Kendall p-value"])
    df_result_south_bay = pd.DataFrame(index=[i for i in range(22)], columns=["Date Delay", "Pearson Coefficient", "Pearson p-value", "Spearman Coefficient", "Spearman p-value", "Kendall Coefficient", "Kendall p-value"])

    # Point Loma
    # Exclude data before 2022/03/14
    date_index = df_point_loma.index[df_point_loma["Date"] == "2022-03-14"].tolist()[0]
    df_point_loma = df_point_loma.iloc[date_index:]


    for i in range(22):
        wind_speed = df_point_loma["avg_wind_speed_m_s"].iloc[:-i].tolist() if i > 0 else df_point_loma["avg_wind_speed_m_s"].tolist()
        viral_genes = df_point_loma["Mean viral gene copies/L"].iloc[i:].tolist()

        df_result_point_loma.at[i, "Date Delay"] = i

        r, p = pearsonr(wind_speed, viral_genes)
        df_result_point_loma.at[i, "Pearson Coefficient"] = r
        df_result_point_loma.at[i, "Pearson p-value"] = p

        rho, p = spearmanr(wind_speed, viral_genes)
        df_result_point_loma.at[i, "Spearman Coefficient"] = rho
        df_result_point_loma.at[i, "Spearman p-value"] = p

        tau, p = kendalltau(wind_speed, viral_genes)
        df_result_point_loma.at[i, "Kendall Coefficient"] = tau
        df_result_point_loma.at[i, "Kendall p-value"] = p


    # Encina
    # Exclude data before 2022/03/14
    date_index = df_encina.index[df_encina["Date"] == "2022-03-14"].tolist()[0]
    df_encina = df_encina.iloc[date_index:]


    for i in range(22):
        wind_speed = df_encina["avg_wind_speed_m_s"].iloc[:-i].tolist() if i > 0 else df_encina["avg_wind_speed_m_s"].tolist()
        viral_genes = df_encina["Mean viral gene copies/L"].iloc[i:].tolist()
        
        df_result_encina.at[i, "Date Delay"] = i

        r, p = pearsonr(wind_speed, viral_genes)
        df_result_encina.at[i, "Pearson Coefficient"] = r
        df_result_encina.at[i, "Pearson p-value"] = p

        rho, p = spearmanr(wind_speed, viral_genes)
        df_result_encina.at[i, "Spearman Coefficient"] = rho
        df_result_encina.at[i, "Spearman p-value"] = p

        tau, p = kendalltau(wind_speed, viral_genes)
        df_result_encina.at[i, "Kendall Coefficient"] = tau
        df_result_encina.at[i, "Kendall p-value"] = p


    # South Bay
    # Exclude data before 2022/03/14
    date_index = df_south_bay.index[df_south_bay["Date"] == "2022-03-14"].tolist()[0]
    df_south_bay = df_south_bay.iloc[date_index:]


    for i in range(22):
        wind_speed = df_south_bay["avg_wind_speed_m_s"].iloc[:-i].tolist() if i > 0 else df_south_bay["avg_wind_speed_m_s"].tolist()
        viral_genes = df_south_bay["Mean viral gene copies/L"].iloc[i:].tolist()
        
        df_result_south_bay.at[i, "Date Delay"] = i

        r, p = pearsonr(wind_speed, viral_genes)
        df_result_south_bay.at[i, "Pearson Coefficient"] = r
        df_result_south_bay.at[i, "Pearson p-value"] = p

        rho, p = spearmanr(wind_speed, viral_genes)
        df_result_south_bay.at[i, "Spearman Coefficient"] = rho
        df_result_south_bay.at[i, "Spearman p-value"] = p

        tau, p = kendalltau(wind_speed, viral_genes)
        df_result_south_bay.at[i, "Kendall Coefficient"] = tau
        df_result_south_bay.at[i, "Kendall p-value"] = p


    # Write to xlsx file
    df_result_point_loma.to_excel(writer, sheet_name="Point Loma")
    df_result_encina.to_excel(writer, sheet_name="Encina")
    df_result_south_bay.to_excel(writer, sheet_name="South Bay")
    print("Correlation Tests completed!")

    writer.sheets["Point Loma"].autofit()
    writer.sheets["Encina"].autofit()
    writer.sheets["South Bay"].autofit()
    writer.close()


if __name__ == "__main__":
    main()