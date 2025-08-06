
# proj imports
from app.api.routes.report_router import router as report_router

# lib imports
from fastapi import FastAPI

app = FastAPI(
    title='Summery Report API',
    description='Insert Sensor data reading, will return summary!'
)
app.include_router(report_router)
