import copy
import logging
from functools import lru_cache
from typing import Optional, List, Tuple

import numpy as np
import pandas as pd
from pydantic import BaseModel, validator
from sklearn.compose import make_column_transformer
from sklearn.ensemble import GradientBoostingRegressor
from sklearn.pipeline import Pipeline, make_pipeline
from sklearn.preprocessing import FunctionTransformer, RobustScaler, StandardScaler

from app.common.models import BasePredictionModel, BaseProvider
from app.common.schemes import ResponseFinalizer
from app.latency.schemes import LatencyModelTrainingRequest, LatencyModelPredictionRequest, LatencyInformationModel, \
    LatencyModelEvaluationRequest
from app.optimization.schemes import ProfileModel


class ClusteringResult(BaseModel):
    latency_clzz: int
    num: int
    min: float
    max: float

    @validator('min')
    def truncate_min_value(cls, val: float):
        return round(val, 2)

    @validator('max')
    def truncate_max_value(cls, val: float):
        return round(val, 2)


class LatencyModelImpl(BasePredictionModel):

    ESTIMATOR_FACTOR: int = 3

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.clustering_results: List[ClusteringResult] = []

        self.previous_data: Optional[pd.DataFrame] = None
        self.previous_stats: Optional[pd.DataFrame] = None

        self.clustering: Pipeline = make_pipeline(
            make_column_transformer(
                (RobustScaler(quantile_range=(0.0, 1.0), with_centering=False), ["latencies"]),
                remainder='drop'
            ),
            FunctionTransformer(LatencyModelImpl.value_transform))

        # column order: scale_outs, throughput_rates, latencies
        self.regressor: Pipeline = make_pipeline(
            make_column_transformer(
                (StandardScaler(), ["scale_outs", "throughput_rates"]),
                remainder='drop'
            ),
            GradientBoostingRegressor(max_depth=10)
        )

    @staticmethod
    def value_transform(x: np.ndarray):
        return np.maximum(0, np.tanh(np.emath.logn(4, x / np.amin(x))))

    def _learn_correct_mapping(self, data: pd.DataFrame):
        clustering_results: List[ClusteringResult] = []
        latencies_clzz: List[int] = list(sorted(data["latencies_clzz"].unique()))
        for latency_clzz in latencies_clzz:
            try:
                selection = data[["latencies"]].values[data[["latencies_clzz"]].values.reshape(-1) == latency_clzz]
                clustering_results.append(ClusteringResult(latency_clzz=latency_clzz,
                                                           num=len(selection),
                                                           min=np.amin(selection),
                                                           max=np.amax(selection)))
            except ValueError:
                pass
        log_string: str = f"Fit-Result of Latency-Clustering: labels={latencies_clzz}"
        for clus_res in sorted(clustering_results, key=lambda res: res.min):
            log_string += f", ClusteringResult({clus_res})"
        logging.info(log_string)
        self.clustering_results = sorted(clustering_results, key=lambda res: res.latency_clzz)
        return data

    def _fit(self, request: LatencyModelTrainingRequest):
        data: pd.DataFrame = pd.DataFrame.from_dict(request.dict(exclude={'job'}))

        if request.append and self.previous_data is not None:
            data = pd.concat([self.previous_data, data], ignore_index=True)

        # fit clustering algorithm
        self.clustering.fit(data)
        data["latencies_norm"] = self.clustering.transform(data)
        data["latencies_clzz"] = np.round(data["latencies_norm"]).astype(int)
        data = self._learn_correct_mapping(data)

        # fit regressor
        self.regressor.steps[-1][-1].n_estimators = LatencyModelImpl.ESTIMATOR_FACTOR * len(data)
        self.regressor.fit(data, data[["latencies"]].values.reshape(-1))
        regression_results: str = LatencyModelImpl.get_regression_results(data[['latencies']].values,
                                                                          self.regressor.predict(data))
        logging.info(f"Fit-Performance of Latency-Regressor "
                     f"(n_estimators={self.regressor.steps[-1][-1].n_estimators}): {regression_results}")

        tuple_list: List[tuple] = [("count", str(len(data)))] + \
                                  [(f"cluster_{clus_res.latency_clzz}", str(clus_res)) for
                                   clus_res in self.clustering_results] + [("regressor", regression_results)]

        stats: pd.DataFrame = pd.DataFrame([dict(tuple_list)])
        if request.append and self.previous_stats is not None:
            stats = pd.concat([self.previous_stats, stats], ignore_index=True)

        self.previous_stats = stats
        self.previous_data = data.drop(columns=['latencies_norm', 'latencies_clzz'])

    def _prepare_response(self, data: pd.DataFrame, current_scale_out: int, previously_valid_scale_outs: List[int]):

        # predict latency with regressor
        data["latencies"] = self.regressor.predict(data)
        # predict latency class with clustering
        data["latencies_norm"] = self.clustering.transform(data)
        data["latencies_clzz"] = np.round(data["latencies_norm"]).astype(int)
        # extract desired columns
        tuple_list: List[Tuple[int, int, float]] = data[["scale_outs", "latencies_clzz", "latencies"]].values.tolist()
        # finalize response and return (current, candidates)
        return ResponseFinalizer.run(tuple_list, current_scale_out,
                                     LatencyInformationModel, 1, previously_valid_scale_outs)

    def evaluate(self, request: LatencyModelEvaluationRequest):
        data: pd.DataFrame = pd.DataFrame([c.dict() for c in request.candidates])
        data["throughput_rate"] = request.predicted_throughput_rate
        data.rename(columns={'scale_out': 'scale_outs',
                             'throughput_rate': 'throughput_rates'}, inplace=True)
        # drop potential duplicates
        data.drop_duplicates(subset=['scale_outs'], inplace=True)
        # return prepared response
        previously_valid_scale_outs: List[int] = [c.scale_out for c in request.candidates if c.is_valid]
        return self._prepare_response(data, request.current.scale_out, previously_valid_scale_outs)

    def predict(self, request: LatencyModelPredictionRequest):
        data: pd.DataFrame = pd.DataFrame([request.dict(exclude={'min_scale_out', 'max_scale_out', 'job'})])
        scale_out_range: List[int] = list(range(request.min_scale_out, request.max_scale_out + 1))
        data.rename(columns={'scale_out': 'scale_outs',
                             'throughput_rate': 'throughput_rates'}, inplace=True)
        data = pd.concat([data] * len(scale_out_range))
        data["scale_outs"] = scale_out_range
        # return prepared response
        return self._prepare_response(data, request.scale_out, list(scale_out_range))

    def alter_profiled_latencies(self, job: str, profiles: List[ProfileModel]):
        profiles_copy: List[ProfileModel] = copy.deepcopy(profiles)

        scale_outs, thr_rates, lats = list(zip(*[(p.configs.scaleout, p.metrics.throughput_rate, p.metrics.latency)
                                                 for p in profiles_copy]))
        scale_outs, thr_rates, lats = list(scale_outs), list(thr_rates), list(lats)
        request: LatencyModelTrainingRequest = LatencyModelTrainingRequest(job=job,
                                                                           scale_outs=scale_outs,
                                                                           throughput_rates=thr_rates,
                                                                           latencies=lats,
                                                                           append=False)
        self.fit(request)

        data: pd.DataFrame = pd.DataFrame.from_dict({"scale_outs": scale_outs,
                                                     "throughput_rates": thr_rates,
                                                     "latencies": lats})
        # predict latency with regressor
        data["latencies"] = self.regressor.predict(data)
        # predict latency class with clustering
        data["latencies_norm"] = self.clustering.transform(data)
        data["latencies_clzz"] = np.round(data["latencies_norm"]).astype(int)
        for profile, latency_norm in zip(profiles_copy, data.latencies_norm.tolist()):
            profile.metrics.latency = latency_norm
        return profiles_copy


class LatencyModelProvider(BaseProvider):
    def __init__(self):
        super().__init__(LatencyModelImpl, "latency")

    @staticmethod
    @lru_cache()
    def get_instance():
        return LatencyModelProvider()
