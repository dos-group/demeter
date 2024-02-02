import asyncio
import json
import os
from asyncio import Task
from pathlib import Path
from typing import Type, Dict

import numpy as np
from fastapi import Request
import dill
from sklearn.metrics import mean_absolute_error, median_absolute_error, mean_absolute_percentage_error, \
    mean_squared_error

from app.common.configuration import GeneralSettings

general_settings: GeneralSettings = GeneralSettings.get_instance()


def _to_task(future, loop):
    if isinstance(future, Task):
        return future
    return loop.create_task(future)


def force_sync(func):
    if asyncio.iscoroutine(func):
        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = asyncio.new_event_loop()
        return loop.run_until_complete(_to_task(func, loop))
    else:
        return func


class BasePredictionModel:

    def __init__(self, target_path: str):
        self.target_path = target_path
        self.is_fitted: bool = False

    @staticmethod
    def get_regression_results(y_true: np.ndarray, y_pred: np.ndarray):
        mean_error: float = mean_absolute_error(y_true, y_pred)
        median_error: float = median_absolute_error(y_true, y_pred)
        mape_error: float = mean_absolute_percentage_error(y_true, y_pred)
        mse_error: float = mean_squared_error(y_true, y_pred)
        return f"MeanError={mean_error:.2f}, " \
               f"MedianError={median_error:.2f}, " \
               f"MAPE={mape_error*100:.2f}%, " \
               f"RMSE={np.sqrt(mse_error):.2f}, " \
               f"MSE={mse_error:.2f}"

    def fit(self, *args, **kwargs):
        self._fit(*args, **kwargs)
        self.is_fitted = True
        self.save()

    def save(self):
        with open(self.target_path, "wb") as file:
            dill.dump(self, file)

    def artifact_exists(self):
        return Path(self.target_path).exists()

    def _fit(self, *args, **kwargs):
        raise NotImplementedError

    def predict(self, *args, **kwargs):
        raise NotImplementedError


class BaseProvider:
    def __init__(self, model_class: Type[BasePredictionModel], search_str: str):
        self.model_class: Type[BasePredictionModel] = model_class
        self.search_str: str = search_str
        self.models: Dict[str, BasePredictionModel] = {}
        self.populate()

    def populate(self):
        # sort filenames, such that mod < raw
        for file_name in sorted(list(os.listdir(general_settings.models_dir))):
            if not file_name.startswith(f"{general_settings.app_env}_"):
                continue
            if f"_{self.search_str}_model.p" in file_name:
                job: str = file_name.split("_")[1]  # first is app_env, second is job_name
                with open(os.path.join(general_settings.models_dir, file_name), "rb") as file:
                    loaded_model: BasePredictionModel = dill.load(file)
                    loaded_model.target_path = loaded_model.target_path.replace("_raw_", "_mod_")
                    if (job not in self.models) or ("_mod_" in file_name):
                        self.models[job] = loaded_model

    def __call__(self, request: Request) -> BasePredictionModel:
        self.populate()  # update cached dict
        awaited_req = force_sync(request.json())
        request_dict: dict = awaited_req if isinstance(awaited_req, dict) else json.loads(awaited_req)
        job: str = request_dict.get("job", None)
        if job is not None:
            if job in self.models:
                return self.models[job]
            else:
                target_path: str = "_".join([general_settings.app_env, job, "raw", self.search_str, "model.p"])
                model = self.model_class(os.path.join(general_settings.models_dir,
                                                      target_path))
                self.models[job] = model
                return model



