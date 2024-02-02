from typing import List, Optional
from pydantic import BaseModel, validator

from app.common.schemes import JobModel, SystemConfigurationModel


class LatencyInformationModel(SystemConfigurationModel):
    latency: float

    @validator('latency')
    def result_check(cls, latency: float):
        return round(latency, 2)


class LatencyModelTrainingRequest(JobModel):
    scale_outs: List[int]
    throughput_rates: List[float]
    latencies: List[float]
    append: Optional[bool] = False

    class Config:
        schema_extra = {
            "example": {
                "scale_outs": [2, 4, 8],
                "throughput_rates": [124.2, 248.4, 496.8],
                "latencies": [10.1, 14.4, 21.9],
                "append": False,
                "job": "TEST"
            }
        }


class LatencyModelPredictionRequest(JobModel):
    min_scale_out: int
    max_scale_out: int
    scale_out: int
    throughput_rate: float

    class Config:
        schema_extra = {
            "example": {
                "min_scale_out": 2,
                "max_scale_out": 24,
                "scale_out": 12,
                "throughput_rate": 124467.1,
                "job": "TEST"
            }
        }


class LatencyModelEvaluationRequest(JobModel):
    current: SystemConfigurationModel
    candidates: List[SystemConfigurationModel]
    predicted_throughput_rate: float

    class Config:
        schema_extra = {
            "example": {
                "current": {
                    "scale_out": 2,
                    "is_best": False,
                    "is_valid": False
                },
                "candidates": [{
                    "scale_out": 8,
                    "is_best": True,
                    "is_valid": True
                }],
                "predicted_throughput_rate": 12300.12,
                "job": "TEST"
            }
        }


class LatencyModelPredictionResponse(BaseModel):
    current: LatencyInformationModel
    candidates: List[LatencyInformationModel]

    class Config:
        schema_extra = {
            "example": {
                "current": {
                    "scale_out": 2,
                    "latency": 5000.0,
                    "is_best": False,
                    "is_valid": False
                },
                "candidates": [{
                    "scale_out": 8,
                    "latency": 1500.0,
                    "is_best": True,
                    "is_valid": True
                }]
            }
        }
