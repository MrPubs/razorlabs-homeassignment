""" Utility functions boiled down to one definition file"""

# lib imports
import duckdb
from typing import List
import pandas as pd


def make_summary_report(sensor_data_parquets: List[str], machines_lookup_csv: str) -> pd.DataFrame:
    '''
    Generate Summery report using SQL
    :param sensor_data_parquets: List of Yesterday's and Today's sensor data in two following parquets
    :param machines_lookup_csv: a csv containing the machines lookup
    :return:
    '''
    yesterday_data, today_data = sensor_data_parquets

    average_summary_view_query = """
     {view_name} AS (
        SELECT
            machine_code,
            coordinate,
            AVG(value) AS avg_value,
            COUNT(*) AS sample_cnt
        FROM parquet_scan('{sensor_data}')
        GROUP BY machine_code, coordinate
    )
    """
    comparison_query = """
    WITH
    {yesterday_view_query},
    {today_view_query},
    diffs as (
        SELECT
            {today_view_name}.machine_code,
            {today_view_name}.coordinate,
            {today_view_name}.avg_value AS today_avg_value,
            {today_view_name}.avg_value - {yesterday_view_name}.avg_value AS increase_in_avg_value,
            {today_view_name}.sample_cnt
        FROM {today_view_name}
        INNER JOIN {yesterday_view_name}
            ON {today_view_name}.machine_code = {yesterday_view_name}.machine_code
                AND {today_view_name}.coordinate = {yesterday_view_name}.coordinate

    ),    
    ranked AS (
        SELECT *,
               ROW_NUMBER() OVER (PARTITION BY machine_code ORDER BY increase_in_avg_value DESC) AS rnk
        FROM diffs
    ),
    machines AS (
        SELECT *
        FROM read_csv_auto('{machines_csv}')
    )

    SELECT 
        machines.machine_name, 
        ranked.coordinate,
        ranked.today_avg_value AS value_avg,
        ranked.increase_in_avg_value AS increase_in_value,
        ranked.sample_cnt
    FROM ranked
    INNER JOIN machines
        ON ranked.machine_code = machines.machine_code
    WHERE rnk = 1 AND increase_in_avg_value > 0
    ORDER BY increase_in_avg_value DESC
    """

    # summary average views
    yesterday_view_name = 'yesterday'
    yesterday_view_query = average_summary_view_query.format(view_name=yesterday_view_name, sensor_data=yesterday_data)
    today_view_name = 'today'
    today_view_query = (average_summary_view_query.format(view_name=today_view_name, sensor_data=today_data))

    # construct final query
    final_query = comparison_query.format(yesterday_view_query=yesterday_view_query,
                                          today_view_query=today_view_query,
                                          yesterday_view_name=yesterday_view_name,
                                          today_view_name=today_view_name,
                                          machines_csv=machines_lookup_csv)
    result = duckdb.sql(final_query)
    return result.df()

if __name__ == '__main__':
    pass