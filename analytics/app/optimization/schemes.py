from __future__ import annotations

import math
import re
from enum import Enum, unique
from typing import Optional, Dict, List, Set, Union
from pydantic import BaseModel, confloat, Field, PositiveInt, validator
import numpy as np
from ax.service.utils.instantiation import ObjectiveProperties
from pydantic.fields import ModelField

from app.common.schemes import JobModel
from app.workload.schemes import TimeSeries


class SearchSpaceModel(BaseModel):
    min_throughput_rate: Optional[int] = Field(default=None, description="step_size=10000")
    max_throughput_rate: Optional[int] = Field(default=None, description="step_size=10000")
    min_scaleout: int = Field(..., description="step_size=4")
    max_scaleout: int = Field(..., description="step_size=4")
    min_checkpoint_interval: float = Field(..., description="step_size=20000")
    max_checkpoint_interval: float = Field(..., description="step_size=20000")
    min_container_cpu: float = Field(..., description="step_size=1.0")
    max_container_cpu: float = Field(..., description="step_size=1.0")
    min_container_memory: int = Field(..., description="step_size=1024")
    max_container_memory: int = Field(..., description="step_size=1024")
    min_task_slots: int = Field(..., description="step_size=1")
    max_task_slots: int = Field(..., description="step_size=1")

    class Config:
        schema_extra = {
            "example": {
                "min_scaleout": 4,
                "max_scaleout": 24,
                "min_checkpoint_interval": 10000,
                "max_checkpoint_interval": 90000,
                "min_container_cpu": 1.0,
                "max_container_cpu": 4.0,
                "min_container_memory": 1024,
                "max_container_memory": 4096,
                "min_task_slots": 1,
                "max_task_slots": 4
            }
        }

    def construct_boundaries_and_step_size(self, name: str):
        def extract(model_field: ModelField):
            extracted_step_size: Optional[float] = None
            flip_elements: bool = False
            if match := re.search(r"step_size=([\d\.]+)", model_field.field_info.description, re.IGNORECASE):
                extracted_step_size = float(match.group(1))
            if match := re.search(r"flip=(True|False)", model_field.field_info.description, re.IGNORECASE):
                flip_elements = bool(match.group(1))
            return extracted_step_size, flip_elements

        min_value, max_value = self.dict().get(f"min_{name}"), self.dict().get(f"max_{name}")
        range_values = [min_value, max_value]
        step_value, flip = extract(self.__fields__.get(f"max_{name}"))
        return tuple(range_values), step_value, flip


class ConstraintModel(BaseModel):
    latency: float = Field(default=0.5, gte=0.5)
    recovery_time: confloat(gt=0)

    class Config:
        schema_extra = {
            "example": {
                "recovery_time": 180
            }
        }

    def get_constraint_definitions(self) -> Set[str]:
        return {f"{k} <= {v}" for k, v in self.get_constraint_definitions_as_dict().items()}

    def get_constraint_definitions_as_dict(self) -> Dict[str, float]:
        return {
            "constraint_latency": np.log1p(self.latency),
            "constraint_recovery_time": np.log(self.recovery_time)
        }


class ObjectiveModel(BaseModel):
    cpu: Optional[float] = None
    memory: Optional[int] = None

    def get_objective_definitions(self, resolver: Dict[str, float]) -> Dict[str, ObjectiveProperties]:
        def threshold_adapter(value: float):
            # The reference point should be set to be slightly worse (10% is reasonable)
            return max(value * 1.1, value + np.amax(np.abs(np.random.normal(0, 0.1, 100000))))

        return {f"objective_{k}": ObjectiveProperties(minimize=True, threshold=threshold_adapter(np.log(resolver[k])))
                for k in self.dict().keys()}


class ConfigurationComparisonEnum(str, Enum):
    GT = "gt"
    LT = "lt"

class ConfigurationModel(BaseModel):
    scaleout: PositiveInt
    checkpoint_interval: PositiveInt
    container_cpu: confloat(gt=0)
    container_memory: PositiveInt
    task_slots: PositiveInt

    def compare(self, comparison_type: ConfigurationComparisonEnum,
                max_cpu: float, max_memory: float, ref: ConfigurationModel, ref_scale_factor: float = 1.0):
        own_cpu_score: float = self.total_cpu / max_cpu
        ref_cpu_score: float = (ref.total_cpu / max_cpu) * ref_scale_factor
        own_memory_score: float = self.total_memory / max_memory
        ref_memory_score: float = (ref.total_memory / max_memory) * ref_scale_factor

        if comparison_type == ConfigurationComparisonEnum.GT:
            return own_cpu_score > ref_cpu_score or \
                own_memory_score > ref_memory_score or \
                (own_cpu_score + own_memory_score) > (ref_cpu_score + ref_memory_score)
        else:
            return own_cpu_score < ref_cpu_score or \
                own_memory_score < ref_memory_score or \
                (own_cpu_score + own_memory_score) < (ref_cpu_score + ref_memory_score)

    @property
    def total_cpu(self):
        return math.ceil(self.scaleout / self.task_slots) * self.container_cpu

    @property
    def total_memory(self):
        return math.ceil(self.scaleout / self.task_slots) * self.container_memory

    @property
    def config_name(self):
        return f"ConfigurationModel(" \
               f"scaleout={self.scaleout}," \
               f"checkpoint_interval={self.checkpoint_interval}," \
               f"container_cpu={self.container_cpu}," \
               f"container_memory={self.container_memory}, " \
               f"task_slots={self.task_slots})"


class MetricsModel(BaseModel):
    throughput_rate: confloat(gt=0)
    latency: confloat(gt=0)
    # current-profile might have no measured recovery time (-1), hence we need to account for that
    recovery_time: Union[confloat(lt=0), confloat(gt=0)]

    class Config:
        schema_extra = {
            "example": {
                "throughput_rate": 30000,
                "latency": 888,
                "recovery_time": 211
            }
        }


class ProfileModel(BaseModel):
    configs: ConfigurationModel
    metrics: MetricsModel

    def get_objective_scores(self):
        return {
            "objective_cpu": np.log(math.ceil(self.configs.scaleout / self.configs.task_slots) * self.configs.container_cpu),
            "objective_memory": np.log(math.ceil(self.configs.scaleout / self.configs.task_slots) * self.configs.container_memory),
        }

    def get_constraint_scores(self):
        return {
            "constraint_latency": np.log1p(self.metrics.latency),
            "constraint_recovery_time": np.log(self.metrics.recovery_time)
        }


class SuggestionModel(BaseModel):
    acqf_value: Optional[float] = None
    configs: ConfigurationModel


class EvaluationRequest(JobModel):
    max_cluster_cpu: confloat(gt=0)
    max_cluster_memory: confloat(gt=0)
    forecast_updates: TimeSeries
    profiles: List[ProfileModel]
    search_space: SearchSpaceModel
    constraints: ConstraintModel
    default_configs: ConfigurationModel
    current_profile: ProfileModel
    objectives: Optional[ObjectiveModel] = ObjectiveModel()

    def get_objective_definitions(self) -> Dict[str, ObjectiveProperties]:
        return self.objectives.get_objective_definitions({
            "cpu": math.ceil(self.search_space.max_scaleout / self.search_space.min_task_slots) * self.search_space.max_container_cpu,
            "memory": math.ceil(self.search_space.max_scaleout / self.search_space.min_task_slots) * self.search_space.max_container_memory
        })


class OptimizationResult(BaseModel):
    production_config: ConfigurationModel
    production_config_is_default: bool
    max_profiling_cpu: float
    max_profiling_memory: float
    cand_profiling_options: List[ConfigurationModel] = []

    @validator('max_profiling_cpu')
    def truncate_max_profiling_cpu(cls, val: float):
        return round(val, 2)

    @validator('max_profiling_memory')
    def truncate_max_profiling_memory(cls, val: float):
        return round(val, 2)


@unique
class ResponseStatusEnum(str, Enum):
    ERROR = "ERROR"
    IGNORE = "IGNORE"
    RESCALE = "RESCALE"
    PROFILE = "PROFILE"


class RescaleResponse(BaseModel):
    status: ResponseStatusEnum
    production_config: SuggestionModel


class ProfileResponse(BaseModel):
    status: ResponseStatusEnum
    profiling_configs: List[SuggestionModel]
