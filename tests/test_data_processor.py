import pandas as pd
import pytest
import os
from app.data_processor.extract import extract_csv
from app.data_processor.transform import transform_sensor_data
from app.data_processor.load import load_parquet
from app.data_processor.analysis import make_summary_report
from app.data_processor.pipeline import onboard_machine_readings_etl, produce_summary_report_etl
import tempfile

# ------- Fixtures --------
@pytest.fixture
def sample_yesterday_csv():
    df = pd.DataFrame([{
        "Tag Name": "RZR__MTR_001",
        "Timestamp": "2024-01-01T00:00:00",
        "Value": 10.642
    }])
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        yield f.name

@pytest.fixture
def sample_today_csv():
    df = pd.DataFrame([{
        "Tag Name": "RZR__MTR_001",
        "Timestamp": "2024-01-02T00:00:00",
        "Value": 11.1
    }])
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        yield f.name

@pytest.fixture
def sample_sensors_csv():
    df = pd.DataFrame([{
        "tag_name": "RZR__MTR_001",
        "machine_code": "CR1",
        "component_code": "Motor",
        "coordinate": "1V"
    }])
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        yield f.name

@pytest.fixture
def sample_machines_csv():
    df = pd.DataFrame([{
        "machine_code": "CR1",
        "machine_name": "Crusher A"
    }])
    with tempfile.NamedTemporaryFile(mode="w+", suffix=".csv", delete=False) as f:
        df.to_csv(f.name, index=False)
        yield f.name


# ------- Tests --------
def test_extract_csv(sample_yesterday_csv):

    df = extract_csv(csv_file=sample_yesterday_csv)
    assert isinstance(df, pd.DataFrame), "Expected to get pandas DataFrame, but didnt"
    assert not df.empty, "Expected DataFrame to have rows but it was empty"
    assert list(df.columns) == list(pd.read_csv(sample_yesterday_csv).columns), "Check for correct columns"


def test_load_parquet(sample_yesterday_csv, tmp_path):

    df = pd.read_csv(sample_yesterday_csv)
    output = os.path.join(tmp_path, 'output_parquet.parquet')
    load_parquet(parquet_path=output, df=df)

    assert os.path.exists(output), 'Expected to find the parquet file, but no file was found!'

    loaded_df = pd.read_parquet(output)
    assert not loaded_df.empty, "Loaded parquet is empty"
    assert loaded_df.shape == df.shape, "Data shape mismatch after loading"
    assert list(loaded_df.columns) == list(df.columns), "Column mismatch"
    assert loaded_df.equals(df), "Data mismatch between input and loaded DataFrame"

def test_transform_sensor_data(sample_yesterday_csv, sample_sensors_csv):

    machine_reading_df = extract_csv(sample_yesterday_csv)
    sensors_lookup_df = extract_csv(sample_sensors_csv)
    df = transform_sensor_data(machine_reading_df=machine_reading_df, sensors_lookup_df=sensors_lookup_df)

    assert isinstance(df, pd.DataFrame), "Expected to get pandas DataFrame, but didnt"
    expected_cols = ['machine_code', 'component_code', 'coordinate', 'sample_time', 'value', 'inserted_at']
    assert set(expected_cols).issubset(df.columns), "Expected columns missing"


def test_make_summery_report(sample_yesterday_csv, sample_today_csv, sample_machines_csv,tmp_path):

    machine_reading_df = extract_csv(sample_yesterday_csv)
    sensors_lookup_df = extract_csv(sample_today_csv)
    sensor_parquets = [load_parquet(os.path.join(tmp_path, 'p1'), machine_reading_df),
                       load_parquet(os.path.join(tmp_path, 'p2'), sensors_lookup_df)]
    df = make_summary_report(sensor_data_parquets=sensor_parquets, machines_lookup_csv=sample_machines_csv)

    assert isinstance(df, pd.DataFrame), "Expected to get pandas DataFrame, but didnt"
    assert not df.empty, "Expected DataFrame to have rows but it was empty"
    expected_cols = ["machine_name", "coordinate", "value_avg", "increase_in_value", "sample_cnt"]
    assert set(expected_cols).issubset(df.columns), "Expected columns missing"


def test_onboard_machine_readings_etl(sample_yesterday_csv, sample_sensors_csv, tmp_path):

    out_parquet = os.path.join(tmp_path, 'p')
    onboard_machine_readings_etl(
        machine_readings_csv=sample_yesterday_csv,
        sensors_csv=sample_sensors_csv,
        output_parquet=out_parquet
    )

    assert os.path.exists(out_parquet), 'Expected to find the parquet file, but no file was found!'

    loaded_df = pd.read_parquet(out_parquet)
    assert not loaded_df.empty, "Loaded parquet is empty"

def test_produce_summary_report_etl(sample_yesterday_csv, sample_today_csv, sample_machines_csv, tmp_path):

    machine_reading_df = extract_csv(sample_yesterday_csv)
    sensors_lookup_df = extract_csv(sample_today_csv)
    sensor_data_parquets = [load_parquet(os.path.join(tmp_path, 'p1'), machine_reading_df),
                            load_parquet(os.path.join(tmp_path, 'p2'), sensors_lookup_df)]

    df = pd.read_csv(sample_machines_csv)
    machines_lookup_csv = os.path.join(tmp_path, 'csv')
    df.to_csv(machines_lookup_csv)
    output_path = os.path.join(tmp_path, "p")
    produce_summary_report_etl(sensor_data_parquets=sensor_data_parquets, machines_lookup_csv=machines_lookup_csv,
                               output_path=output_path)

    assert os.path.exists(output_path), 'Expected to find the parquet file, but no file was found!'

    loaded_df = pd.read_parquet(output_path)
    assert not loaded_df.empty, "Loaded parquet is empty"
