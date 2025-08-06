""" Offloading methods to different sources boiled down to one definition file"""

# lib imports
import pandas as pd
import os


def load_parquet(parquet_path: str, df: pd.DataFrame) -> str:
    '''
    Load df to parquet at given path
    :param parquet_path: desired parquet file
    :param df: df to load as parquet
    :return: None!
    '''
    # Save to parquet
    try:

        # Check for target file existence
        if os.path.exists(parquet_path):
            print(f"[WARNING] file '{parquet_path}' already exists, overwriting existing file!")

        # try saving file
        print(f"[INFO] saving '{parquet_path}'..")
        df.to_parquet(parquet_path, index=False)
        print(f"[INFO] successfully saved as parquet: '{parquet_path}'!")
        return parquet_path

    except Exception as e:
        print(f"[ERROR] failed to save '{parquet_path}': {e}")
        return ''


if __name__ == '__main__':
    pass