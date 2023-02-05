from enum import Enum
from typing import List

from pydantic import BaseModel


class Extra(str, Enum):
    ignore: str = "ignore"


class HourTempData(BaseModel):
    hour: int
    temp: int
    condition: str

    class Config:
        extra = Extra.ignore


class ForecastDateData(BaseModel):
    date: str
    hours: List[HourTempData]

    class Config:
        extra = Extra.ignore


class LocalityData(BaseModel):
    name: str

    class Config:
        extra = Extra.ignore


class GeoObjectData(BaseModel):
    locality: LocalityData

    class Config:
        extra = Extra.ignore


class CityData(BaseModel):
    geo_object: GeoObjectData
    forecasts: List[ForecastDateData]

    class Config:
        extra = Extra.ignore
