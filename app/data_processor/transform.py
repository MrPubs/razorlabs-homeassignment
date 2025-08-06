""" Transformation methods from different sources boiled down to one definition file"""

# lib imports
import pandas as pd
from time import time

def transform_sensor_data(machine_reading_df: pd.DataFrame, sensors_lookup_df: pd.DataFrame) -> pd.DataFrame:
    '''
    Transforms machine reading data frame, and a sensors lookup table, to a parquet containing the relevant
    operations per defined in the docs of the assignment.
    :param machine_reading_df: machine reading data frame of a day
    :param sensors_lookup_df: a lookup table used to describe a sensor
    :return: a pandas dataframe
    '''

    # Normalize columns to lowercase and underscore style (sensors lookup style)
    machine_reading_df = machine_reading_df.rename(columns={
        'Tag Name': 'tag_name',
        'Timestamp': 'timestamp',
        'Value': 'value'
    })
    machine_reading_df['sample_time'] = \
        pd.to_datetime(machine_reading_df['timestamp'], utc=True).astype('int64') // 1000  # Epoch in microseconds

    # force value field to be float64
    machine_reading_df['value'] = pd.to_numeric(machine_reading_df['value'], errors='coerce')

    # Join relevant cols based on tag_name
    join_col = ['tag_name']
    relevant_machine_reading_cols = ['sample_time', 'value'] + join_col
    relevant_sensors_lookup_cols = ['machine_code', 'component_code', 'coordinate'] + join_col
    joined_df = machine_reading_df[relevant_machine_reading_cols].merge(
                sensors_lookup_df[relevant_sensors_lookup_cols], on='tag_name', how='left').drop(columns=join_col)

    # Add insertion time
    joined_df['inserted_at'] = int(time() * 1_000_000)

    # Sort columns
    sorted_columns = [
        'machine_code',
        'component_code',
        'coordinate',
        'sample_time',
        'value',
        'inserted_at']

    return joined_df[sorted_columns]


if __name__ == '__main__':
    pass