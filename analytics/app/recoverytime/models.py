import copy
import logging
import math
from functools import lru_cache
from typing import List, Tuple, Union
import numpy as np
from scipy import integrate
from sklearn.linear_model import LinearRegression, RidgeCV

from app.common.models import BaseProvider, BasePredictionModel
from app.common.schemes import ResponseFinalizer
from app.recoverytime.schemes import RecoveryTimeModelPredictionRequest, RecoveryTimeInformationModel, \
    RecoveryTimeModelTrainingRequest, RecoveryTimeModelEvaluationRequest
from app.workload.models import WorkloadModelImpl
from app.workload.schemes import TimeSeries, Observation, WorkloadModelPredictionRequest


class RecoveryTimeModelImpl(BasePredictionModel):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.capacity_model = RidgeCV()

    def _fit(self, request: RecoveryTimeModelTrainingRequest):
        scale_outs: np.ndarray = np.array(request.scale_outs).reshape(-1, 1)
        max_throughput_rates: np.ndarray = np.array(request.max_throughput_rates).reshape(-1, 1)

        self.capacity_model.fit(scale_outs, max_throughput_rates.reshape(-1))
        logging.info("Fit-Performance of Capacity-Regressor: " +
                     RecoveryTimeModelImpl.get_regression_results(max_throughput_rates,
                                                                  self.capacity_model.predict(scale_outs)))

    @staticmethod
    def subsample(data: np.ndarray, bin_count: int):
        slices = np.linspace(0, len(data), bin_count + 1, True).astype(int)
        counts = np.diff(slices)

        avg_binned_data: np.ndarray = np.add.reduceat(data, slices[:-1]) / counts
        return avg_binned_data.reshape(-1).tolist()

    @staticmethod
    def eval_single(total_workload: TimeSeries, throughput_rate: float,
                    partial_results_list: List[Tuple[int, int, float]]):

        # Integrating using Samples
        def integrate_func(observations: List[Observation]):
            t_stamps, tp_rates = TimeSeries.unfold(observations)
            result = integrate.simpson(np.array(tp_rates), np.array(t_stamps))
            return result * (1 / throughput_rate)

        while True:
            start_time = partial_results_list[-1][0]
            end_time = partial_results_list[-1][1]

            sliced_workload: TimeSeries = TimeSeries.select(total_workload, start_time, end_time)

            if sliced_workload.count > 0:
                time_comp = integrate_func(sliced_workload.observations)

                partial_results_list[-1] = (start_time, end_time, time_comp)
                if time_comp < 1:
                    break

                partial_results_list.append((end_time, end_time + math.ceil(time_comp), 0))
            else:
                break

        _, _, time_comps = zip(*partial_results_list)
        catch_up: float = sum(time_comps)
        return catch_up

    def _process(self, request: Union[RecoveryTimeModelPredictionRequest, RecoveryTimeModelEvaluationRequest],
                 current_scale_out: int,
                 scale_out_range: List[int],
                 model: WorkloadModelImpl,
                 previously_valid_scale_outs: List[int]):

        past_workload: TimeSeries = copy.deepcopy(request.workload)
        past_timestamps, past_throughput_rates = TimeSeries.unfold(past_workload)
        prediction_period: float = request.prediction_period_in_s
        max_rt: float = request.max_recovery_time

        fut_workload = model.predict(WorkloadModelPredictionRequest(workload=past_workload,
                                                                    job=request.job,
                                                                    prediction_period_in_s=prediction_period))

        total_workload: TimeSeries = TimeSeries.merge(past_workload, fut_workload)

        # start, end, predicted workload, time it took
        results_list: List[Tuple[int, int, float]] = [(
            int(past_timestamps[-1] - request.last_checkpoint),
            int(past_timestamps[-1] + math.ceil(request.downtime)),
            0
        )]

        # scale_out, predicted recovery time
        tuple_list: List[Tuple[int, float]] = []
        for scale_out in list(scale_out_range):
            max_throughput_rate: float = self.capacity_model.predict(np.array([[scale_out]])).reshape(-1)[0]
            catch_up: float = RecoveryTimeModelImpl.eval_single(total_workload, max_throughput_rate, list(results_list))
            tuple_list.append((scale_out, catch_up + request.downtime))

        # finalize response and return (current, candidates)
        current, candidates = ResponseFinalizer.run(tuple_list, current_scale_out,
                                                    RecoveryTimeInformationModel, max_rt, previously_valid_scale_outs)

        # calculate predicted throughput rate
        fut_timestamps, fut_throughput_rates = TimeSeries.unfold(fut_workload)
        averaged_bins: List[float] = RecoveryTimeModelImpl.subsample(np.array(fut_throughput_rates), request.bin_count)

        # lastly: calculate slope
        reg: LinearRegression = LinearRegression()
        reg.fit(fut_timestamps.reshape(-1, 1), fut_throughput_rates.reshape(-1, 1))
        slope: float = np.amax(reg.coef_)  # there is only one coefficient

        return current, candidates, max(averaged_bins), slope

    def evaluate(self, request: RecoveryTimeModelEvaluationRequest, model: WorkloadModelImpl):
        scale_out_range: List[int] = [c.scale_out for c in request.candidates]
        previously_valid_scale_outs: List[int] = [c.scale_out for c in request.candidates if c.is_valid]
        return self._process(request,
                             request.current.scale_out,
                             sorted(list(set(scale_out_range))),
                             model,
                             previously_valid_scale_outs)

    def predict(self, request: RecoveryTimeModelPredictionRequest, model: WorkloadModelImpl):
        scale_out_range: List[int] = list(range(request.min_scale_out, request.max_scale_out + 1))
        return self._process(request, request.scale_out, scale_out_range, model, list(scale_out_range))


class RecoveryTimeModelProvider(BaseProvider):
    def __init__(self):
        super().__init__(RecoveryTimeModelImpl, "recoverytime")

    @staticmethod
    @lru_cache()
    def get_instance():
        return RecoveryTimeModelProvider()
