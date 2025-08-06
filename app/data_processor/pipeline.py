""" Master Pipeline Orchestrator, handles lifecycle of the pipeline and its flows"""

# onboard machine reading
from .extract import extract_csv
from .transform import transform_sensor_data
from .load import load_parquet

# summary report production
from .analysis import make_summary_report

# lib imports
import os
from typing import List
import logging


def onboard_machine_readings_etl(machine_readings_csv: str, sensors_csv: str, output_parquet: str) -> str:

    # Early terminate if exists
    if os.path.exists(output_parquet):
        print(f"[INFO] Skipping onboard etl for file '{output_parquet}' already exists!")  # TODO logging?
        return ''
    print(f"[INFO] Onboarding process for '{machine_readings_csv}' started..")

    # Extract
    df_machine = extract_csv(machine_readings_csv)
    df_lookup = extract_csv(sensors_csv)

    # Transform
    df_transformed = transform_sensor_data(machine_reading_df=df_machine,
                                           sensors_lookup_df=df_lookup)

    # Load
    onboarded_data = load_parquet(parquet_path=output_parquet, df=df_transformed)
    return onboarded_data


def produce_summary_report_etl(sensor_data_parquets: List[str], machines_lookup_csv: str, output_path: str) -> str:

    # Transform
    df_report = make_summary_report(sensor_data_parquets=sensor_data_parquets,
                                    machines_lookup_csv=machines_lookup_csv)

    # Load
    summary_report = load_parquet(parquet_path=output_path, df=df_report)
    return summary_report

if __name__ == '__main__':
    pass

    # dir for the general data
    data_dir = r"C:\\Users\\zivg\\Documents\\Repos\\razorlabs-homeassignment\\data"

    # paths to files
    machine_readings_csvs_list = [os.path.join(data_dir, 'csv', '2024-01-01.csv'),
                                  os.path.join(data_dir, 'csv', '2024-01-02.csv')]
    sensors_csv = os.path.join(data_dir, 'csv', 'Sensors.csv')
    output_parquets = [os.path.join(data_dir, 'parquet', '2024-01-01.parquet'),
                       os.path.join(data_dir, 'parquet', '2024-01-02.parquet')]

    for machine_readings_csv, output_parquet in zip(machine_readings_csvs_list, output_parquets):

        onboard_machine_readings_etl(machine_readings_csv=machine_readings_csv,
                                     sensors_csv=sensors_csv,
                                     output_parquet=output_parquet)

    machines_lookup_csv = os.path.join(data_dir, 'csv', 'Machines.csv')
    output_summary = os.path.join(data_dir, 'parquet', 'summary-2024-01-02.parquet')
    produce_summary_report_etl(sensor_data_parquets=output_parquets,
                               machines_lookup_csv=machines_lookup_csv,
                               output_path=output_summary)
