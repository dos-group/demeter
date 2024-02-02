import copy
import logging
import pickle
from collections import OrderedDict
from functools import lru_cache
from typing import Any, Tuple, List, Dict, Union, Optional

import dill
import numpy as np
import sklearn
from pydantic import BaseModel
from scipy import signal
import pandas as pd
from multiprocessing import Pool, Manager
import pmdarima.arima as pm

from app.common.configuration import GeneralSettings
from app.common.models import BasePredictionModel, BaseProvider
from app.workload.schemes import WorkloadModelTrainingRequest, TimeSeries, WorkloadModelPredictionRequest

general_settings: GeneralSettings = GeneralSettings().get_instance()


class ArimaModelWrapper(BaseModel):
    model: Any
    step_size: int
    last_learned_timestamp: Optional[Union[int, float]]
    cached_workload: TimeSeries

    def compute_target_timestamps(self, workload: TimeSeries, offset: int = 0) -> np.ndarray:
        start_time: int
        if self.last_learned_timestamp <= workload.start_time <= (self.last_learned_timestamp + self.step_size):
            start_time = self.last_learned_timestamp
        else:
            start_time = (workload.start_time - self.step_size)

        end_time: int = workload.end_time + offset + 1

        return np.arange(start_time + self.step_size,
                         end_time,
                         self.step_size).astype(int).reshape(-1)


class WorkloadModelImpl(BasePredictionModel):
    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.models: OrderedDict[int, ArimaModelWrapper] = OrderedDict()
        self.step_size: int = general_settings.workload_prediction_step_size
        self.num_models: int = int(general_settings.workload_prediction_period_in_s / self.step_size)
        self.smooth_args: Tuple[int, int] = (51, 3)
        logging.info(f"Init {self.__class__.__name__} with: num_models={self.num_models}, "
                     f"step_size={self.step_size}, "
                     f"smooth_args={self.smooth_args}")
        self.predicted_workload: Optional[TimeSeries] = None

    @staticmethod
    def _process_workload(workload: TimeSeries, step_size: int, smooth_args: Tuple[int, int]):
        def _smooth(data: np.ndarray, window_size: int, poly: int) -> np.ndarray:
            corr_ws = min(window_size, len(data))
            corr_ws = corr_ws if (corr_ws % 2) == 1 else (corr_ws - 1)
            corr_poly = min(poly, corr_ws - 1)
            if len(data):
                return signal.savgol_filter(data, corr_ws, corr_poly)
            else:
                return data

        def _avg(data: np.ndarray, s_size: int) -> np.ndarray:
            return pd.Series(data) \
                .rolling(window=s_size, min_periods=1, center=True, win_type="exponential").mean() \
                .interpolate(method='linear', limit_direction='both') \
                .values.reshape(-1)

        timestamps, throughput_rates = TimeSeries.unfold(workload)
        smooth_throughput_rates: np.ndarray = _smooth(throughput_rates, *smooth_args)
        avg_throughput_rates: np.ndarray = _avg(smooth_throughput_rates, int(2 * step_size))
        return TimeSeries.fold(timestamps, avg_throughput_rates)

    @staticmethod
    def _fit_routine(args: tuple):
        manager_dict, manager_lock, model_id, workload, step_size = args

        timestamps, throughput_rates = TimeSeries.unfold(workload)

        model_step_size: int = int(model_id * step_size)
        indices: np.ndarray = (workload.count - 1) - np.arange(0, workload.count, model_step_size)
        indices = indices[::-1].astype(int)

        model = pm.auto_arima(throughput_rates[indices],
                              error_action="ignore",
                              out_of_sample_size=int(len(indices) * 0.2),
                              scoring="mae")
        model_wrapper: ArimaModelWrapper = ArimaModelWrapper(model=model,
                                                             step_size=model_step_size,
                                                             last_learned_timestamp=workload.end_time,
                                                             cached_workload=TimeSeries.fold([], []))
        with manager_lock:
            manager_dict[model_id] = pickle.dumps(model_wrapper)

    @staticmethod
    def _predict_routine(workload: TimeSeries, models: Dict[int, ArimaModelWrapper], step_size: int) -> List[float]:
        prediction_horizon: int = int(len(models) * step_size) + 1
        df: pd.DataFrame = pd.DataFrame(columns=list(models.keys()),
                                        index=list(range(workload.end_time, workload.end_time + prediction_horizon, 1)),
                                        dtype=float)

        for model_id, model_wrapper in models.items():
            merged_workload: TimeSeries = TimeSeries.merge(model_wrapper.cached_workload, workload)
            merged_workload = TimeSeries.select(merged_workload,
                                                model_wrapper.cached_workload.start_time,
                                                merged_workload.end_time)
            timestamps, throughput_rates = TimeSeries.unfold(merged_workload)

            if df.isnull().values.all():
                df.iloc[0, :] = throughput_rates[-1]

            target_timestamps: np.ndarray = model_wrapper.compute_target_timestamps(merged_workload)
            target_indices: np.ndarray = TimeSeries.indices_for_timestamps(merged_workload, target_timestamps)
            if len(target_indices):
                # update with most recent observations
                model_wrapper.model.update(throughput_rates[target_indices])
                model_wrapper.last_learned_timestamp = timestamps[target_indices[-1]]
                merged_workload = TimeSeries.select(merged_workload,
                                                    timestamps[target_indices[-1]],
                                                    merged_workload.end_time)

            model_wrapper.cached_workload = merged_workload

            # predict
            target_timestamps: np.ndarray = model_wrapper.compute_target_timestamps(merged_workload,
                                                                                    offset=prediction_horizon - 1)
            if len(target_timestamps):
                predictions: np.ndarray = model_wrapper.model.predict(n_periods=len(target_timestamps))
                df.loc[target_timestamps, [model_id]] = predictions.reshape(-1, 1)

        df = df.dropna(axis='columns', how='all')
        return df.ewm(min_periods=1, ignore_na=True, adjust=False, span=5, axis=1).mean() \
                   .interpolate(method='linear', limit_direction='both', axis=0) \
                   .rolling(min_periods=1, window=120, center=True, axis=0).mean() \
                   .values[1:, -1].reshape(-1).tolist()

    def eval_prediction_accuracy(self, workload: TimeSeries):
        if self.predicted_workload is not None:
            start_time: int = max(self.predicted_workload.start_time, workload.start_time)
            end_time: int = min(self.predicted_workload.end_time, workload.end_time)
            sub_predicted_workload: TimeSeries = TimeSeries.select(self.predicted_workload,
                                                                   start_time, end_time)
            if sub_predicted_workload.count:
                _, true_thr_rates = TimeSeries.unfold(TimeSeries.select(workload, start_time, end_time))
                _, pred_thr_rates = TimeSeries.unfold(sub_predicted_workload)
                reg_res: str = WorkloadModelImpl.get_regression_results(true_thr_rates,
                                                                        pred_thr_rates)
                logging.info(f"Predict-Performance of Workload-Regressor "
                             f"(some {sub_predicted_workload.count}s in the past cycle): {reg_res}")

    def _fit(self, request: WorkloadModelTrainingRequest):
        self.models.clear()

        processed_workload: TimeSeries = self._process_workload(request.workload, self.step_size, self.smooth_args)

        manager_dict: Dict[int, bytes]
        with Manager() as manager:
            manager_dict = manager.dict()
            manager_lock = manager.Lock()
            with Pool() as p:
                p.map(WorkloadModelImpl._fit_routine,
                      [(manager_dict, manager_lock, model_id, processed_workload, self.step_size)
                       for model_id in np.arange(1, self.num_models + 1)])

            for key, value in sorted(list(manager_dict.items()), key=lambda item: item[0]):
                self.models[key] = pickle.loads(value)

    def predict(self, request: WorkloadModelPredictionRequest, save: bool = True) -> TimeSeries:
        if not len(self.models):
            raise sklearn.exceptions.NotFittedError

        processed_workload: TimeSeries = self._process_workload(request.workload, self.step_size, self.smooth_args)
        self.eval_prediction_accuracy(processed_workload)

        new_thr_rates: List[float] = self._predict_routine(processed_workload, self.models, self.step_size)
        new_timestamps: List[float] = [(processed_workload.end_time or 0) + (offset * processed_workload.step_size)
                                       for offset in range(1, len(new_thr_rates) + 1)]
        self.predicted_workload = TimeSeries.fold(new_timestamps[:request.prediction_period_in_s],
                                                  new_thr_rates[:request.prediction_period_in_s])
        # save again state, so that the update of the models is captured
        if save:
            self.save()
        return copy.deepcopy(self.predicted_workload)


class WorkloadModelProvider(BaseProvider):
    def __init__(self, search_str: str = "workload"):
        super().__init__(WorkloadModelImpl, search_str)

    @staticmethod
    @lru_cache()
    def get_instance(search_str: str = "workload"):
        return WorkloadModelProvider(search_str)
