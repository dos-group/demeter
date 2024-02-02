import os
import pathlib
from functools import lru_cache
from typing import Optional, List

from pydantic import BaseSettings, root_validator

# https://fastapi.tiangolo.com/advanced/settings/?h=envir
root_dir: str = str(pathlib.Path(__file__).parents[1].absolute())


class GeneralSettings(BaseSettings):
    root_dir: Optional[str] = root_dir
    logging_level: Optional[str] = "DEBUG"
    app_env: Optional[str] = "DEV"
    models_dir: Optional[str] = "artifacts/models"
    log_dir: Optional[str] = "artifacts/logs"
    job_names: List[str] = ["ADS", "CARS"]
    workload_prediction_step_size: int = 30
    workload_prediction_period_in_s: int = 600
    workload_prediction_bin_count: int = 10
    workload_prediction_buffer: float = 1.10
    profiling_timeout: float = 600 # 10 minutes
    max_num_support_models: int = 2
    optimization_min_improvement: float = 0.05
    optimization_base_factor: float = 0.95
    optimization_chunk_size: int = 500 # reasonable for 16GB of total memory
    optimization_const_scaler: float = 1.25
    optimization_obj_scaler: float = 1.25

    @root_validator(skip_on_failure=True)
    def validate_profiling_timeout(cls, values):
        workload_prediction_period_in_s: int = values["workload_prediction_period_in_s"]
        profiling_timeout: float = values["profiling_timeout"]
        if profiling_timeout > workload_prediction_period_in_s:
            raise ValueError("Evaluation-Timeout must be smaller / equal the workload forecasting horizon!")
        return values

    @staticmethod
    @lru_cache()
    def get_instance():
        general_settings: GeneralSettings = GeneralSettings()
        general_settings.models_dir = os.path.join(general_settings.root_dir, general_settings.models_dir)
        general_settings.log_dir = os.path.join(general_settings.root_dir, general_settings.log_dir)

        if not os.path.exists(general_settings.models_dir):
            os.makedirs(general_settings.models_dir)

        if not os.path.exists(general_settings.log_dir):
            os.makedirs(general_settings.log_dir)

        return general_settings
