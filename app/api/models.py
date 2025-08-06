from pydantic import BaseModel
from typing import List


class MachineMetadata(BaseModel):
    """
    Represents the aggregated sensor data for a specific machine and coordinate.
    """
    machine_name: str  # Name of the machine (e.g. "Crusher A")
    coordinate: str  # Coordinate/tag name this data refers to (e.g. "3V")
    value_avg: float  # Average value of sensor readings
    increase_in_value: float  # Increase in average value compared to previous day
    sample_cnt: int  # Number of samples used to compute the average


class ReportMetadata(BaseModel):
    """
    Metadata describing the context of the report.
    """
    date: str  # Date string (inferred from today’s data filename)
    yesterday_data: str  # Filename of the CSV used as "yesterday's" input
    today_data: str  # Filename of the CSV used as "today's" input


class ReportResponse(BaseModel):
    """
    API response containing a list of machine summaries and report metadata.
    """
    machines: List[MachineMetadata]  # Summary for each machine/coordinate pair
    metadata: ReportMetadata  # Information about the input data used
