""" Extraction methods from different sources boiled down to one definition file"""

# lib imports
import pandas as pd
import os

def extract_csv(csv_file: str) -> pd.DataFrame:
    '''
    Extract a csv file to a dataframe
    :param csv_file: csv file to extract
    :return: a pandas dataframe
    '''
    try:
        df = pd.read_csv(csv_file)
        print(f"[INFO] Loaded CSV: '{csv_file}' ({df.shape[0]} rows, {df.shape[1]} columns)")
        return df

    except Exception as e:
        print(f"[ERROR] Failed to read CSV from '{csv_file}': {e}")
        raise


if __name__ == '__main__':
    pass