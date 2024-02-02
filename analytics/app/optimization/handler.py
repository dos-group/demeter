import copy
import logging
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple

import numpy as np

from app.common.configuration import GeneralSettings
from app.latency.models import LatencyModelImpl
from app.optimization.models.common import CustomModelListGP
from app.optimization.models.optimizer import OptimizerBO
from app.optimization.models.rgpe import OptimizerRGPE
from app.optimization.models.task import OptimizerTASK
from app.optimization.schemes import ConfigurationModel, EvaluationRequest, OptimizationResult, ProfileModel, \
    ResponseStatusEnum, SuggestionModel
from app.recoverytime.models import RecoveryTimeModelImpl
from app.workload.models import WorkloadModelImpl
from app.workload.schemes import TimeSeries, WorkloadModelPredictionRequest


general_settings: GeneralSettings = GeneralSettings.get_instance()

def _filter_by_num_init(profiles: List[Tuple[ProfileModel, bool]], num_init: int) -> List[ProfileModel]:
    has_enough: bool = sum([is_chunk_fit for _, is_chunk_fit in profiles])
    has_diversity: bool = (any([is_chunk_fit for p, is_chunk_fit in profiles if round(p.metrics.latency) == 0]) and
                           any([is_chunk_fit for p, is_chunk_fit in profiles if round(p.metrics.latency) == 1]))
    if has_enough >= num_init and has_diversity:
        profiles = [(p, is_chunk_fit) for p, is_chunk_fit in profiles if is_chunk_fit]
    profiles: List[ProfileModel] = [p for p, _ in profiles]
    return profiles


def _get_support_model(request: EvaluationRequest, thr_chunk_key: Tuple[float, float],
                       obj_list: List[Tuple[ProfileModel, bool]]) -> Optional[Tuple[Tuple[float, float], list, any]]:
    if len(obj_list) < 3:
        return None
    else:
        num_init: int = 3
        seed: int = sum(thr_chunk_key)
        initial_profiles: List[ProfileModel] = _filter_by_num_init(obj_list, num_init)
        logging.debug(f"thr_chunk_key={thr_chunk_key}, #profiles_before={len(obj_list)}, #profiles_after={len(initial_profiles)}")
        optimizer: OptimizerBO = OptimizerTASK(request,
                                               num_init=num_init,
                                               seed=seed,
                                               normalize=True,
                                               initial_profiles=initial_profiles)
        if optimizer.best_model is not None:
            return thr_chunk_key, initial_profiles, optimizer.best_model
        return None

def _get_bottlenecked_configs_at_lower_rates(target_thr_chunk_key: Tuple[float, float],
                                             history_dict: Dict[Tuple[float, float],
                                             List[Tuple[ProfileModel, bool]]]) -> List[ConfigurationModel]:
    bottlenecked_configs_at_lower_rates: List[ConfigurationModel] = []
    for p_list in history_dict.values():
        for t in p_list:
            p, _ = t
            # only consider smaller throughput rates
            # has this profiling run experience a bottleneck, in terms of measured latencies?
            if (p.metrics.throughput_rate <= target_thr_chunk_key[0] and
                    round(p.metrics.latency) == 1 and
                    all([p.configs.config_name != c.config_name for c in bottlenecked_configs_at_lower_rates])):
                bottlenecked_configs_at_lower_rates.append(p.configs)
    return bottlenecked_configs_at_lower_rates


def _get_stable_configs_at_higher_rates(target_thr_chunk_key: Tuple[float, float],
                                             history_dict: Dict[Tuple[float, float],
                                             List[Tuple[ProfileModel, bool]]]) -> List[ConfigurationModel]:
    stable_configs_at_higher_rates: List[ConfigurationModel] = []
    for p_list in history_dict.values():
        for t in p_list:
            p, _ = t
            # only consider bigger throughput rates
            # has this profiling run experience stability, in terms of measured latencies?
            if (p.metrics.throughput_rate >= target_thr_chunk_key[1] and
                    round(p.metrics.latency) == 0 and
                    all([p.configs.config_name != c.config_name for c in stable_configs_at_higher_rates])):
                stable_configs_at_higher_rates.append(p.configs)
    return stable_configs_at_higher_rates


def _get_target_model(request: EvaluationRequest,
                      result_list: List[Tuple[Tuple[float, float], List[ProfileModel], CustomModelListGP]],
                      history_dict: Dict[Tuple[float, float], List[Tuple[ProfileModel, bool]]],
                      target_thr_chunk_key: Tuple[float, float]) -> Tuple[OptimizerBO, List[ConfigurationModel], List[ConfigurationModel]]:
    target_optimizer: OptimizerBO
    bottlenecked_configs_at_lower_rates: List[ConfigurationModel] = _get_bottlenecked_configs_at_lower_rates(
        target_thr_chunk_key, history_dict)
    stable_configs_at_higher_rates: List[ConfigurationModel] = _get_stable_configs_at_higher_rates(
        target_thr_chunk_key, history_dict)
    seed: int = sum(target_thr_chunk_key)
    num_init: int = 1 if len(result_list) else 3
    normalize: bool = len(result_list)
    obj_list: List[Tuple[ProfileModel, bool]] = history_dict.get(target_thr_chunk_key, [])
    initial_profiles: List[ProfileModel] = _filter_by_num_init(obj_list, 3)
    logging.debug(f"target_thr_chunk_key={target_thr_chunk_key}, #profiles_before={len(obj_list)}, #profiles_after={len(initial_profiles)}")

    if len(result_list):
        _, _, support_model_tuple_of_lists = zip(*result_list)
        target_optimizer: OptimizerBO = OptimizerRGPE(request,
                                                      list(support_model_tuple_of_lists),
                                                      num_init=num_init,
                                                      seed=seed,
                                                      normalize=normalize,
                                                      initial_profiles=initial_profiles,
                                                      exp_config={"acquisition_options": {
                                                          "cache_root": False
                                                      }})
    else:
        target_optimizer: OptimizerBO = OptimizerTASK(request,
                                                      num_init=num_init,
                                                      seed=seed,
                                                      normalize=normalize,
                                                      initial_profiles=initial_profiles)
    return target_optimizer, bottlenecked_configs_at_lower_rates, stable_configs_at_higher_rates


def _prepare_data(request: EvaluationRequest, workload_model: WorkloadModelImpl, latency_model: LatencyModelImpl,
                  save_workload_model: bool = True) -> Tuple[list, dict, ProfileModel, Tuple[float, float]]:
    # what is the predicted throughput rate in the future?
    prediction_request = WorkloadModelPredictionRequest(
        job=request.job,
        workload=request.forecast_updates,
        prediction_period_in_s=general_settings.workload_prediction_period_in_s
    )
    # calculate predicted throughput rate
    _, fut_throughput_rates = TimeSeries.unfold(
        workload_model.predict(prediction_request, save=save_workload_model))
    averaged_bins: List[float] = RecoveryTimeModelImpl.subsample(np.array(fut_throughput_rates),
                                                                 general_settings.workload_prediction_bin_count)
    max_averaged_bins: float = round(max(averaged_bins) * general_settings.workload_prediction_buffer, 2)
    logging.debug(f"fut_throughput_rates_max_averaged_bins={max_averaged_bins}")
    # set boundaries according to past, current, and prospective observations
    all_thr_information: List[float] = ([obj.metrics.throughput_rate for obj in request.profiles] +
                                        [request.current_profile.metrics.throughput_rate] +
                                        [max_averaged_bins])
    request.search_space.min_throughput_rate = min(all_thr_information)
    request.search_space.max_throughput_rate = max(all_thr_information)
    # extract associated step-size
    boundaries, step_size, _ = request.search_space.construct_boundaries_and_step_size("throughput_rate")
    min_throughput_rate, max_throughput_rate = boundaries
    # create throughput chunks, which will be used to create models
    thr_steps: List[float] = np.arange(min_throughput_rate, max_throughput_rate + (2 * step_size),
                                       step=step_size).astype(float).round(2).tolist()
    thr_step_chunks: List[Tuple[float, float]] = list(zip(thr_steps[:-1], thr_steps[1:]))
    logging.info(f"#Thr-Chunks={len(thr_step_chunks)}; Min-Chunk={thr_step_chunks[0]}; Max-Chunk={thr_step_chunks[-1]}")

    target_thr_chunk_key: Tuple[float, float] = next((t for t in thr_step_chunks if t[0] >= max_averaged_bins))
    logging.info(f"target_thr_chunk_key={target_thr_chunk_key}")

    profiles: List[ProfileModel] = request.profiles
    current_profile: ProfileModel = request.current_profile
    tmp_comb_profiles: List[ProfileModel] = profiles + [current_profile]
    if len(tmp_comb_profiles) > 1:
        # alter latencies, i.e. detect what is normal / abnormal
        logging.debug(f"Latencies before normalization: {[round(p.metrics.latency, 2) for p in tmp_comb_profiles]}")
        tmp_comb_profiles = latency_model.alter_profiled_latencies(request.job, tmp_comb_profiles)
        logging.debug(f"Latencies after normalization: {[round(p.metrics.latency, 2) for p in tmp_comb_profiles]}")
        profiles = tmp_comb_profiles[:len(profiles)]
        current_profile = tmp_comb_profiles[-1]
    else:
        # this will only ever happen at first request(s), when there are no profiles yet
        current_profile.metrics.latency = 0

    ext_history_dict: Dict[Tuple[float, float], List[Tuple[float, ProfileModel, bool]]] = OrderedDict()
    for obj in profiles:
        res_tups: List[Tuple[float, Tuple[float, float], bool]] = []
        for t in thr_step_chunks:
            # Either direct chunk fit,
            # or good latencies and part of higher chunk
            # or bad latencies and part of lower chunk
            direct_chunk_fit: bool = t[0] <= round(obj.metrics.throughput_rate, 2) <= t[1]
            if (direct_chunk_fit or
                    (round(obj.metrics.throughput_rate, 2) > t[1] and round(obj.metrics.latency) == 0) or
                    (round(obj.metrics.throughput_rate, 2) < t[0] and round(obj.metrics.latency) == 1)
            ):
                res_tups.append((abs(obj.metrics.throughput_rate - t[1]), t, direct_chunk_fit))
        for res_tup in res_tups:
            diff, thr_chunk_key, direct_chunk_fit = res_tup
            ext_history_dict[thr_chunk_key] = ext_history_dict.get(thr_chunk_key, []) + [(diff, obj, direct_chunk_fit)]

    history_dict: Dict[Tuple[float, float], List[Tuple[ProfileModel, bool]]] = OrderedDict()
    for chunk_key, tuple_list in ext_history_dict.items():
        profile_list: List[Tuple[ProfileModel, bool]] = []
        for diff, obj, direct_chunk_fit in sorted(tuple_list, key=lambda tup: tup[0]):  # sort by diff, then iterate
            if all([obj.configs.config_name != p.configs.config_name for (p, _) in profile_list]):
                profile_list.append((obj, direct_chunk_fit))
        assert len(profile_list) == len(set([p.configs.config_name for (p, _) in profile_list])), "Duplicate profiles!"
        history_dict[chunk_key] = profile_list
    info_log_dict = {t: len(history_dict.get(t, [])) for t in thr_step_chunks}
    logging.info(f"#Used-Profiles: {sum(list(info_log_dict.values()))}; Prepared Chunk-Dictionary: {info_log_dict}")

    result_list: List[Tuple[Tuple[float, float], List[ProfileModel], CustomModelListGP]] = []
    for chunk_key, obj_list in sorted(history_dict.items(), key=lambda tup: sum(tup[0])):
        if len(result_list) == general_settings.max_num_support_models:
            continue
        if chunk_key[0] >= target_thr_chunk_key[1]:
            result: Optional[Tuple[Tuple[float, float], List[ProfileModel], CustomModelListGP]] = _get_support_model(request, chunk_key, obj_list)
            result_list += [result] if result is not None else []
    if len(result_list):
        logging.info(f"Number of selected Support-Models: {len(result_list)}")

    return result_list, history_dict, current_profile, target_thr_chunk_key


def handle_rescale_request(raw_request: EvaluationRequest,
                           workload_model: WorkloadModelImpl, latency_model: LatencyModelImpl) -> dict:
    request: EvaluationRequest = copy.deepcopy(raw_request)
    result_list, history_dict, current_profile, target_thr_chunk_key = _prepare_data(request,
                                                                                     workload_model,
                                                                                     latency_model,
                                                                                     save_workload_model=True)
    optimizer, _, _ = _get_target_model(request, result_list, history_dict, target_thr_chunk_key)
    opt_result: OptimizationResult = optimizer.get_optimized_configs(request, current_profile)
    status: str = ResponseStatusEnum.RESCALE
    if opt_result.production_config.config_name == request.current_profile.configs.config_name:
        status = ResponseStatusEnum.IGNORE
    # return results
    return {"status": status,
            "production_config": SuggestionModel(configs=opt_result.production_config).dict()}


def handle_profile_request(raw_request: EvaluationRequest,
                           workload_model: WorkloadModelImpl, latency_model: LatencyModelImpl) -> dict:
    request: EvaluationRequest = copy.deepcopy(raw_request)
    start_eval_time: float = time.time() - 10

    result_list, history_dict, current_profile, target_thr_chunk_key = _prepare_data(request,
                                                                                     workload_model,
                                                                                     latency_model,
                                                                                     save_workload_model=True)
    optimizer, bot_configs, stab_configs = _get_target_model(request, result_list,
                                                             history_dict, target_thr_chunk_key)
    opt_result: OptimizationResult = optimizer.get_optimized_configs(request, current_profile, write_log=False)

    # Finally: retrieve profiling suggestions
    suggestions: List[SuggestionModel] = optimizer.get_suggestions(request, opt_result, start_eval_time,
                                                                   bot_configs, stab_configs)
    # return results
    return {"status": ResponseStatusEnum.PROFILE,
            "profiling_configs": [sug.dict() for sug in suggestions]}
