from typing import List, Union, Optional, Any
import numpy as np
import pandas as pd
from pydantic import BaseModel, validator
import math

from app.common.schemes import JobModel


class Observation(BaseModel):
    timestamp: Union[int, float]
    value: Optional[float]


class TimeSeries(BaseModel):
    observations: List[Observation]
    count: Optional[int]

    class Config:
        schema_extra = {
            "example": {
                "observations": [{"timestamp": i + 1, "value": v} for i, v in enumerate(np.random.rand(300))],
                "count": 300
            }
        }

    @validator('count', always=True)
    def set_count(cls, count: Optional[int], values: dict):
        return count or len(values["observations"])

    @property
    def start_time(self):
        return self.observations[0].timestamp if self.count else None

    @property
    def end_time(self):
        return self.observations[-1].timestamp if self.count else None

    @property
    def step_size(self):
        return 1 if self.count <= 1 else ((self.end_time - self.start_time) / (self.count - 1))

    @staticmethod
    def create_example(number: int, offset: int = 1):
        timestamps, values = np.arange(offset, offset + number), np.random.rand(number)
        return TimeSeries.fold(timestamps, values)

    @classmethod
    def unfold(cls, timeseries: Any):
        target = timeseries.observations if hasattr(timeseries, "observations") else timeseries
        if len(target):
            timestamps, values = zip(*[(o.timestamp, o.value or math.nan) for o in target])
        else:
            timestamps, values = [], []

        workload: pd.Series = pd.Series(data=values, index=timestamps)
        workload = workload.loc[workload.first_valid_index():]
        workload = workload.interpolate(method='linear', limit_direction='both')

        timestamps_arr = np.array(list(workload.index)).reshape(-1)
        values_arr = workload.values.reshape(-1)
        return timestamps_arr, values_arr

    @classmethod
    def fold(cls, timestamps: Union[np.ndarray, List[Union[int, float]]], values: Union[np.ndarray, List[float]]):
        if isinstance(timestamps, np.ndarray):
            timestamps = timestamps.reshape(-1).tolist()
        elif isinstance(timestamps, tuple):
            timestamps = list(timestamps)

        if isinstance(values, np.ndarray):
            values = values.reshape(-1).tolist()
        elif isinstance(values, tuple):
            values = list(values)

        observations: List[Observation] = []
        for timestamp, value in zip(timestamps, values):
            observations.append(Observation(timestamp=timestamp, value=value))
        return cls(observations=observations, count=len(observations))

    @classmethod
    def merge(cls, *timeseries):
        timeseries = [ts for ts in timeseries if ts.count]
        if not len(timeseries):
            return TimeSeries.fold([], [])

        start_time: int = min(ts.start_time for ts in timeseries)
        end_time: int = max(ts.end_time for ts in timeseries)

        df: pd.DataFrame = pd.DataFrame(columns=["value"],
                                        index=list(range(start_time, end_time + 1, 1)),
                                        dtype=float)

        # we sort by end_time, assuming that higher timestamps mean more recent timeseries objects
        for ts in sorted(timeseries, key=lambda ts: ts.end_time):
            timestamps_arr, values_arr = TimeSeries.unfold(ts)
            df.loc[timestamps_arr.reshape(-1), ["value"]] = values_arr.reshape(-1, 1)

        # now interpolate possible nan-values
        values: np.ndarray = df.interpolate(method='linear', limit_direction='both', axis=0)["value"].values.reshape(-1)
        timestamps: np.ndarray = np.array(list(df.index))
        return TimeSeries.fold(timestamps, values)

    @classmethod
    def select(cls, timeseries: Any, start_time: int, end_time: int):
        timestamps, throughput_rates = TimeSeries.unfold(timeseries)
        # Note that contrary to usual python slices, both the start and the stop are included
        workload: pd.Series = pd.Series(data=throughput_rates, index=timestamps).loc[start_time:end_time]
        return TimeSeries.fold(np.array(list(workload.index)), workload.values.reshape(-1))

    @classmethod
    def indices_for_timestamps(cls, timeseries: Any, target_timestamps: np.ndarray):
        timestamps, _ = TimeSeries.unfold(timeseries)
        timestamps = timestamps.reshape(-1).tolist()
        target_indices: List[int] = []
        for target_timestamp in target_timestamps:
            if target_timestamp in timestamps:
                target_indices.append(timestamps.index(target_timestamp))
        return np.array(target_indices)

    def __repr__(self):
        return self.__str__()

    def __str__(self):
        non_null_count: int = sum(int(observation.value is not None) for observation in self.observations)
        return f"TimeSeries(all={self.count}, " \
               f"non_null={non_null_count}, " \
               f"null={self.count - non_null_count})"


class WorkloadModelTrainingRequest(JobModel):
    workload: TimeSeries

    class Config:
        schema_extra = {
            "example": {
                "workload": TimeSeries.create_example(2400),
                "job": "TEST"
            }
        }


class WorkloadModelPredictionRequest(JobModel):
    workload: TimeSeries
    prediction_period_in_s: int

    class Config:
        schema_extra = {
            "example": {
                "workload": TimeSeries.create_example(60),
                "prediction_period_in_s": 60,
                "job": "TEST"
            }
        }


class WorkloadModelPredictionResponse(BaseModel):
    workload: TimeSeries

    class Config:
        schema_extra = {
            "example": {
                "workload": TimeSeries.create_example(3)
            }
        }
