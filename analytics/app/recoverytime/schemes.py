from typing import List, Optional

from pydantic import BaseModel, validator

from app.common.schemes import JobModel, SystemConfigurationModel
from app.workload.schemes import TimeSeries


class RecoveryTimeInformationModel(SystemConfigurationModel):
    recovery_time: float

    @validator('recovery_time')
    def result_check(cls, recovery_time: float):
        return round(recovery_time, 2)


class RecoveryTimeModelTrainingRequest(JobModel):
    scale_outs: List[int]
    max_throughput_rates: List[float]

    class Config:
        schema_extra = {
            "example": {
                "scale_outs": [2, 4, 8],
                "max_throughput_rates": [124.2, 248.4, 496.8],
                "job": "TEST"
            }
        }


class RecoveryTimeModelEvaluationRequest(JobModel):
    current: SystemConfigurationModel
    candidates: List[SystemConfigurationModel]
    workload: TimeSeries
    bin_count: Optional[int] = 4
    prediction_period_in_s: int
    downtime: float
    last_checkpoint: float
    max_recovery_time: float

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
                "workload": TimeSeries.create_example(60),
                "prediction_period_in_s": 300,
                "downtime": 10.0,
                "last_checkpoint": 90.0,
                "max_recovery_time": 240,
                "job": "TEST"
            }
        }


class RecoveryTimeModelPredictionRequest(JobModel):
    min_scale_out: int
    max_scale_out: int
    workload: TimeSeries
    scale_out: int
    bin_count: Optional[int] = 4
    prediction_period_in_s: int
    downtime: float
    last_checkpoint: float
    max_recovery_time: float

    class Config:
        schema_extra = {
            "example": {
                "min_scale_out": 2,
                "max_scale_out": 24,
                "workload": TimeSeries.create_example(60),
                "scale_out": 4,
                "prediction_period_in_s": 300,
                "downtime": 10.0,
                "last_checkpoint": 90.0,
                "max_recovery_time": 240,
                "job": "TEST"
            }
        }


class RecoveryTimeModelPredictionResponse(BaseModel):
    current: RecoveryTimeInformationModel
    candidates: List[RecoveryTimeInformationModel]
    predicted_throughput_rate: float
    slope: float

    class Config:
        schema_extra = {
            "example": {
                "current": {
                    "scale_out": 2,
                    "recovery_time": 123.1,
                    "is_best": False,
                    "is_valid": False
                },
                "candidates": [{
                    "scale_out": 8,
                    "recovery_time": 153.1,
                    "is_best": True,
                    "is_valid": True
                }],
                "predicted_throughput_rate": 350000.2,
                "slope": 0.78
            }
        }
