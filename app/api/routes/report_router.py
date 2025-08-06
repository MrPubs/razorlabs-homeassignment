
# proj imports
from app.data_processor.pipeline import produce_summary_report_etl, onboard_machine_readings_etl
from app.api.models import ReportMetadata, ReportResponse, MachineMetadata

# lib imports
from fastapi import APIRouter, UploadFile, File, HTTPException
from typing import List
import os
import tempfile
import pandas as pd

# router
router = APIRouter()

@router.post('/report', response_model=ReportResponse)
async def make_report(
        machine_readings_csv: List[UploadFile] = File(...)
):
    '''
    create a summary report through api request
    :param machine_readings_csv: List of two following sensor data readings, the second being the more recent reading
    :return: Response following the ReportResponse model.
    '''

    # Check for correct file type
    for f in machine_readings_csv:
        if f.content_type != 'text/csv':
            raise HTTPException(status_code=400, detail=f"Invalid file type for {f.filename}")

    # Check if metadata exists
    sensors_csv = os.path.join("data", "csv", "Sensors.csv")
    machines_csv = os.path.join("data", "csv", "Machines.csv")
    if not os.path.exists(sensors_csv) or not os.path.exists(machines_csv):
        raise HTTPException(status_code=500, detail="Required metadata files not found in system!")

    # Create Workspace
    with tempfile.TemporaryDirectory() as tmpdir:

        # Save input CSVs temporarily
        yesterday_csv_path = os.path.join(tmpdir, "yesterday.csv")
        today_csv_path = os.path.join(tmpdir, "today.csv")
        with open(yesterday_csv_path, "wb") as f:
            f.write(await machine_readings_csv[0].read()) # Save yesterday
        with open(today_csv_path, "wb") as f:
            f.write(await machine_readings_csv[1].read()) # Save today

        # Save uploaded files to tempdir
        yesterday_parquet = os.path.join(tmpdir,'yesterday_sensor_data.parquet')
        today_parquet = os.path.join(tmpdir,'today_sensor_data.parquet')
        onboard_machine_readings_etl(machine_readings_csv=yesterday_csv_path, sensors_csv=sensors_csv,
                                     output_parquet=yesterday_parquet)
        onboard_machine_readings_etl(machine_readings_csv=today_csv_path, sensors_csv=sensors_csv,
                                     output_parquet=today_parquet)

        output_parquets = [yesterday_parquet, today_parquet]
        summary_parquet = os.path.join(tmpdir, 'summary_report.parquet')
        produce_summary_report_etl(sensor_data_parquets=output_parquets,
                                   machines_lookup_csv=machines_csv,
                                   output_path=summary_parquet)

        # JSONify
        summary_date = machine_readings_csv[1].filename.split('.')[0]
        df = pd.read_parquet(summary_parquet)
        records = df.to_dict(orient="records")

        # Create list of Pydantic machine models efficiently
        machines = [MachineMetadata(**rec) for rec in records]

        # Construct response
        response = ReportResponse(
            machines=machines,
            metadata=ReportMetadata(
                date=summary_date,
                yesterday_data=machine_readings_csv[0].filename,
                today_data=machine_readings_csv[1].filename
            )
        )

    return response

if __name__ == '__main__':
    pass