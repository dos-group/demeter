import concurrent.futures
import copy
import logging
import time
from collections import OrderedDict
from typing import Dict, List, Optional, Tuple, Union
import numpy as np
import pandas as pd
from ax.core import ObservationFeatures
from ax.core.base_trial import TrialStatus
from ax.modelbridge.modelbridge_utils import extract_objective_thresholds, extract_objective_weights, \
    extract_outcome_constraints, \
    extract_search_space_digest
from ax.modelbridge.random import RandomModelBridge

from ax.service.ax_client import AxClient
from ax.service.utils.instantiation import ObjectiveProperties

from app.common.configuration import GeneralSettings
from app.optimization.models.common import CustomModelListGP
from app.optimization.models.embedder import EmbedderBO
from app.optimization.models.evaluator import EvaluatorBO
from app.optimization.schemes import ConfigurationComparisonEnum, ConfigurationModel, OptimizationResult, \
    ProfileModel, SuggestionModel, EvaluationRequest

general_settings: GeneralSettings = GeneralSettings.get_instance()

class OptimizerBO:

    def __init__(self, **kwargs):
        self.best_model: Optional[CustomModelListGP] = None

        self.exp_config: dict = kwargs.get("exp_config", {})
        self.normalize: bool = kwargs.get("normalize", True)

        self.seed: int = int(kwargs.get("seed", 42))
        np.random.seed(self.seed)

        self.num_init = kwargs.get("num_init", 3)

        # they will all be set in the respective subclasses
        self.ax_client: Optional[AxClient] = None
        self.embedder: Optional[EmbedderBO] = None
        self.evaluator: Optional[EvaluatorBO] = None

    @staticmethod
    def get_acqf_value(ax_client_copy: AxClient, observation_feature: ObservationFeatures):
        model_bridge = ax_client_copy.generation_strategy.model
        transformed_gen_args = model_bridge._get_transformed_gen_args(
            search_space=ax_client_copy.experiment.search_space,
        )
        search_space_digest = extract_search_space_digest(
            search_space=transformed_gen_args.search_space,
            param_names=model_bridge.parameters,
        )
        objective_weights = extract_objective_weights(
            objective=ax_client_copy.experiment.optimization_config.objective,
            outcomes=model_bridge.outcomes,
        )
        objective_thresholds = None
        if hasattr(ax_client_copy.experiment.optimization_config, "objective_thresholds"):
            objective_thresholds = extract_objective_thresholds(
                objective_thresholds=ax_client_copy.experiment.optimization_config.objective_thresholds,
                objective=ax_client_copy.experiment.optimization_config.objective,
                outcomes=model_bridge.outcomes
            )
        outcome_constraints = extract_outcome_constraints(
            outcome_constraints=ax_client_copy.experiment.optimization_config.outcome_constraints,
            outcomes=model_bridge.outcomes
        )
        acq_options = copy.deepcopy(model_bridge.model.acquisition_options)
        acqf_values = [ax_client_copy.generation_strategy.model.evaluate_acquisition_function(
                    observation_features=[observation_feature],
                    search_space_digest=search_space_digest,
                    objective_weights=objective_weights,
                    objective_thresholds=objective_thresholds,
                    outcome_constraints=outcome_constraints,
                    acq_options=acq_options
                )]
        acqf_values = sum([[v] if isinstance(v, float) else v for v in acqf_values], [])
        return acqf_values.pop()

    @staticmethod
    def sum_row(row: pd.Series, sum_row_divisor: float = 1.0, sum_row_scale_const: bool = False):
        log_val: float = sum(row.values)
        if sum_row_scale_const:
            is_lat_const: bool = any(["latency" in k for k in list(row.keys())])
            log_val = OptimizerBO.rescale_log_value(log_val, general_settings.optimization_const_scaler, is_lat_const)
        return log_val / sum_row_divisor

    @staticmethod
    def rescale_log_value(log_value: float, scaler: float, is_lat_const: bool) -> float:
        value = np.expm1(log_value) if is_lat_const else np.exp(log_value)
        value *= scaler
        log_value = np.log1p(value) if is_lat_const else np.log(value)
        return log_value

    @staticmethod
    def filter_by_constraints(df: pd.DataFrame, request: EvaluationRequest,
                              const_keys: List[str], is_prod: bool = False) -> pd.DataFrame:
        defs: Dict[str, float] = request.constraints.get_constraint_definitions_as_dict()
        for const in const_keys:
            if not len(df) or is_prod:
                continue

            const_cols: List[str] = [f"{const}_mean", f"{const}_std"]
            df.loc[:, f"{const}_adjusted"] = df.loc[:, const_cols].apply(OptimizerBO.sum_row,
                                                                         axis=1,
                                                                         sum_row_scale_const=True)
            df = df.drop(index=df[df[f"{const}_adjusted"] > defs[const]].index)
            df = df.drop(columns=const_cols + [f"{const}_adjusted"])

        return df.copy()

    @staticmethod
    def sort_by_objective_values(df: pd.DataFrame, request: EvaluationRequest,
                                 obj_keys: List[str], is_prod: bool = False) -> pd.DataFrame:
        defs: Dict[str, ObjectiveProperties] = request.get_objective_definitions()
        for obj in obj_keys:
            obj_cols: List[str] = [f"{obj}_mean", f"{obj}_std"]
            # first: add mean and std, to get uncertain and relatively stable estimate
            df.loc[:, f"{obj}_adjusted"] = df.loc[:, [f"{obj}_mean"]].apply(OptimizerBO.sum_row,
                                                                            axis=1)
            if len(df) > 1 and not is_prod:
                # next: only consider options with XX% / quantile more than the best performer
                frac_offset_log_value: float = OptimizerBO.rescale_log_value(df[f"{obj}_adjusted"].min(),
                                                                             general_settings.optimization_obj_scaler,
                                                                             False)
                quantile_log_value: float = df[f"{obj}_adjusted"].quantile(
                    q=(general_settings.optimization_obj_scaler - 1), interpolation="linear")
                df.loc[:, f"{obj}_adjusted_valid"] = df.loc[:, [f"{obj}_adjusted"]].apply(
                    lambda row: sum(row.values) >= max(frac_offset_log_value, quantile_log_value), axis=1)
            else:
                df.loc[:, f"{obj}_adjusted_valid"] = True
            # finally: compute thresholded value, as before
            df.loc[:, f"{obj}_adjusted"] = df.loc[:, [f"{obj}_adjusted", f"{obj}_std"]].apply(OptimizerBO.sum_row,
                                                                                              axis=1,
                                                                                              sum_row_divisor=defs[obj].threshold)
            df = df.drop(columns=obj_cols)

        df.loc[:, "comb_obj"] = df.loc[:, [f"{obj}_adjusted" for obj in obj_keys]].apply(OptimizerBO.sum_row, axis=1)
        df.loc[:, "comb_obj_valid"] = df.loc[:, [f"{obj}_adjusted_valid" for obj in obj_keys]].apply(
            lambda row: all(row.values), axis=1
        )

        df = df[df["comb_obj_valid"]]
        df = df.drop(columns=[f"{obj}_adjusted_valid" for obj in obj_keys] + ["comb_obj_valid"])

        df = df.sort_values(by=["comb_obj"])
        df = df.reset_index(drop=True)
        return df.copy()

    def _process_predictions(self, obs_feats: List[ObservationFeatures],
                             request: EvaluationRequest, write_log: bool = True, is_prod: bool = False) -> pd.DataFrame:
        write_log and logging.debug(f"Number of observation features to predict performance for: {len(obs_feats)}")
        agg_dict_list: List[dict] = []
        # Divide input data into smaller chunks, otherwise processing does not work
        cs: int = general_settings.optimization_chunk_size
        chunks: List[List[ObservationFeatures]] = [obs_feats[i:i + cs] for i in range(0, len(obs_feats), cs)]
        for idx, chunk in enumerate(chunks):
            write_log and logging.debug(f"Process chunk {idx + 1:03}/{len(chunks):03} of observation features...")
            model_bridge = copy.deepcopy(self.ax_client).generation_strategy.model
            mean, covar = model_bridge.predict(chunk)
            metric_keys = list(mean.keys())
            for i, obs_feat in enumerate(chunk):
                agg_dict_list.append({
                    "parameters": obs_feat.parameters,
                    **{f"{k}_mean": mean[k][i] for k in metric_keys},
                    **{f"{k}_std": (covar[k][k][i] ** 0.5) for k in metric_keys},
                })
        agg_df = pd.DataFrame(agg_dict_list)
        assert len(agg_df) == len(obs_feats), "Error processing the chunks of observation features!"

        all_metric_keys: List[str] = ["_".join(n.split("_")[:-1]) for n in list(agg_df.columns)]
        const_keys = list(set([k for k in all_metric_keys if k.startswith("constraint")]))
        obj_keys = list(set([k for k in all_metric_keys if k.startswith("objective")]))

        agg_df = self.filter_by_constraints(agg_df, request, const_keys, is_prod=is_prod)
        if not len(agg_df):
            write_log and logging.debug("Constraints are not fulfilled, can not optimize...")
            return agg_df
        write_log and logging.debug(f"Number of configurations that fulfill constraints: {len(agg_df)}")
        agg_df = self.sort_by_objective_values(agg_df, request, obj_keys, is_prod=is_prod)
        return agg_df

    def _prepare_optimization_result(self, request: EvaluationRequest, next_configs: ConfigurationModel,
                                     comparison_type: Optional[ConfigurationComparisonEnum],
                                     ref_scale_factor: Optional[float],
                                     scaler: Optional[float] = None) -> OptimizationResult:
        # calculate resource options and remaining resources if next_configs would be used
        resource_options, _, max_cpu, max_memory = self.calculate_remaining_resources(
            request.max_cluster_cpu, request.max_cluster_memory,
            current_configs=next_configs, scaler=scaler
        )
        # further alter resource options, if needed
        if all([arg is not None for arg in [comparison_type, ref_scale_factor]]):
            resource_options = [ro for ro in resource_options if
                                ro.compare(
                                    comparison_type, max_cpu, max_memory,
                                    next_configs,
                                    ref_scale_factor=ref_scale_factor
                                )]
        # return the optimization result
        return OptimizationResult(
            production_config=next_configs,
            production_config_is_default=next_configs.config_name == request.default_configs.config_name,
            max_profiling_cpu=max_cpu,
            max_profiling_memory=max_memory,
            cand_profiling_options=resource_options
        )

    def get_optimized_configs(self, request: EvaluationRequest, current_profile: ProfileModel,
                              write_log: bool = True) -> OptimizationResult:
        def uniform_log_statement(rescale: bool, reason: str, action: str):
            write_log and logging.info(f"Rescale='{rescale}' | Reason='{reason}' | Action='{action}'")

        current_config_is_default: bool = current_profile.configs.config_name == request.default_configs.config_name

        # <----------------------------------------------------------------------------------------> #
        # FIRST: Allows the current config for good latencies?
        # <----------------------------------------------------------------------------------------> #
        if round(current_profile.metrics.latency) == 1:
            # If latencies are bad, we scale to the default (max. resources) and do additional profiling
            uniform_log_statement(True, "Current config has bad latencies", "Choose default config")
            # we only want to potentially profile bigger configurations
            return self._prepare_optimization_result(request, request.default_configs,
                                                     comparison_type=ConfigurationComparisonEnum.GT,
                                                     ref_scale_factor=0.75)

        # <----------------------------------------------------------------------------------------> #
        # SECOND: Do we even have a model that can make predictions?
        # <----------------------------------------------------------------------------------------> #
        if isinstance(self.ax_client.generation_strategy.model, RandomModelBridge):
            # In this case, we just do profiling
            uniform_log_statement(False, "RandomModelBridge can not make predictions", "Do nothing")
            return self._prepare_optimization_result(request, current_profile.configs,
                                                     comparison_type=None,
                                                     ref_scale_factor=None)

        # <----------------------------------------------------------------------------------------> #
        # THIRD: Will the current config be able to cope with the prospective workload?
        # If not, can we find an alternative? Worst Case: go back to default
        # <----------------------------------------------------------------------------------------> #
        # this first call will possibly filter
        curr_df: pd.DataFrame = self._process_predictions(
            [ObservationFeatures(parameters=current_profile.configs.dict())],
            request, write_log=False
        )
        curr_df_can_handle: bool = len(curr_df) or current_config_is_default
        # this second call will not filter, hence we get actual values back
        curr_df: pd.DataFrame = self._process_predictions(
            [ObservationFeatures(parameters=current_profile.configs.dict())],
            request, write_log=False, is_prod=True
        )
        # get all potential profiling / optimization candidates
        cand_profiling_options: List[ConfigurationModel] = self._prepare_optimization_result(
            request, current_profile.configs,
            comparison_type=None,
            ref_scale_factor=None,
            scaler=1.0
            ).cand_profiling_options
        # get model predictions and validate them
        obs_feats: List[ObservationFeatures] = [ObservationFeatures(parameters=c.dict()) for c in
                                                cand_profiling_options]
        cand_df: pd.DataFrame = self._process_predictions(obs_feats, request, write_log=write_log)

        next_best_configs: ConfigurationModel
        ref_scale_factor: float
        if curr_df_can_handle:
            ref_scale_factor = 0.25
            if not len(cand_df):
                # If there are no other valid ones, we keep the current config and do additional profiling
                uniform_log_statement(False, "There are no other valid configs at the moment",
                                      "Do nothing")
                next_best_configs = current_profile.configs
            else:
                # Else: If we previously found valid configs, we check if there is a substantial improvement
                best_obj = cand_df.iloc[0]["comb_obj"]
                curr_obj = curr_df.iloc[0]["comb_obj"]

                obj_relative_diff: float = abs(max(best_obj, curr_obj) - min(best_obj, curr_obj)) / abs(curr_obj)
                if best_obj < curr_obj and obj_relative_diff <= general_settings.optimization_min_improvement:
                    uniform_log_statement(False,
                                          "The current config is still estimated to be most efficient",
                                          "Do nothing")
                    next_best_configs = current_profile.configs
                else:
                    uniform_log_statement(True,
                                          "The current config is no longer estimated to be most efficient",
                                          f"Choose the better config with combined objective value {best_obj}")
                    next_best_configs = ConfigurationModel(**cand_df.iloc[0]["parameters"])
        else:
            ref_scale_factor = 0.75
            if not len(cand_df):
                # if the current config can not deal with the prospective workload, we go back to default
                uniform_log_statement(True, "Current config is estimated to fail with prospective workload",
                                      "Choose default config")
                next_best_configs = request.default_configs
            else:
                best_obj = cand_df.iloc[0]["comb_obj"]
                uniform_log_statement(True, "Current config is estimated to fail with prospective workload",
                                      f"Choose the better config with combined objective value {best_obj}")
                next_best_configs = ConfigurationModel(**cand_df.iloc[0]["parameters"])
        # Finally return
        return self._prepare_optimization_result(request, next_best_configs,
                                                 comparison_type=ConfigurationComparisonEnum.GT,
                                                 ref_scale_factor=ref_scale_factor)

    @staticmethod
    def has_resources(total_cpu: float, max_cluster_cpu: float,
                      total_memory: float, max_cluster_memory: float) -> Tuple[bool, float]:
        has_resources: bool = total_cpu <= max_cluster_cpu and total_memory <= max_cluster_memory
        ratios: Tuple[float, float] = (total_cpu / max_cluster_cpu, total_memory / max_cluster_memory)
        ratios = (max(0.1, ratios[0]), max(0.1, ratios[1]))
        ratios = (max(ratios[0], 1 / ratios[0]), max(ratios[1], 1 / ratios[1]))
        return has_resources, sum(ratios) / len(ratios)
    def calculate_remaining_resources(self, max_cluster_cpu: float, max_cluster_memory: float,
                                      current_configs: Optional[ConfigurationModel] = None,
                                      scaler: Optional[float] = None,
                                      resource_options: Optional[List[ConfigurationModel]] = None,
                                      write_log: bool = True):
        if scaler is None:
            scaler = general_settings.optimization_base_factor ** \
                     len(self.ax_client.experiment.trials_by_status[TrialStatus.COMPLETED])

        t_c: float = max_cluster_cpu
        t_m: float = max_cluster_memory
        if current_configs is not None:
            t_c -= current_configs.total_cpu
            t_m -= current_configs.total_memory
        t_c *= scaler
        t_m *= scaler

        if resource_options is None or not len(resource_options):
            resource_options = [o for o in self.embedder.options
                                if self.has_resources(o.total_cpu, t_c, o.total_memory, t_m)[0]]
            if current_configs is not None:
                resource_options = [ro for ro in resource_options if ro.config_name != current_configs.config_name]

        no_duplicate_options: List[ConfigurationModel] = [o for o in resource_options
                                                          if all([o.config_name != p.configs.config_name
                                                                  for p in self.evaluator.profiles])]
        write_log and logging.info(f"Number of candidate configurations: {len(resource_options)}")
        return resource_options, no_duplicate_options, t_c, t_m

    def get_suggestions(self, request: EvaluationRequest, opt_result: OptimizationResult, start_eval_time: float,
                        bottlenecked_configs_at_lower_rates: List[ConfigurationModel],
                        stable_configs_at_higher_rates: List[ConfigurationModel]):
        end_eval_time: float = start_eval_time + general_settings.profiling_timeout
        suggestions: List[SuggestionModel] = []
        stopped: bool = False

        logging.debug(f"#bottlenecked_configs_at_lower_rates: {len(bottlenecked_configs_at_lower_rates)}")
        logging.debug(f"#stable_configs_at_higher_rates: {len(stable_configs_at_higher_rates)}")

        def get_remaining_time(as_string: bool = True):
            value: Union[float, str] = max(0.0, end_eval_time - time.time())
            if as_string:
                value = f"{value:.2f}s"
            return value

        is_sobol_generation_strategy: bool = isinstance(self.ax_client.generation_strategy.model, RandomModelBridge)
        while not stopped:
            params: dict
            trial_idx: int

            f: concurrent.futures.Future = concurrent.futures.ThreadPoolExecutor().submit(self.ax_client.get_next_trial)
            try:
                params, trial_idx = f.result(timeout=get_remaining_time(as_string=False))
            except BaseException as err:
                if isinstance(err, concurrent.futures.TimeoutError):
                    # terminate if optimization took too long
                    logging.info(f"Defined timeout of {general_settings.profiling_timeout}s has been reached, "
                                     f"will stop searching now!")
                else:
                    logging.error(f"Encountered error while fetching a new trial! ### {err} ###")
                stopped = True
                continue

            write_log: bool = trial_idx % (100 if is_sobol_generation_strategy else 1) == 0

            observation_feature = ObservationFeatures(parameters=params)
            gs_model_class_name: str = self.ax_client.generation_strategy.model.__class__.__name__
            write_log and logging.info(f"Fetched trial No. {trial_idx}...(GS-model={gs_model_class_name})")

            acqf_value: float = -1
            if len(self.evaluator.profiles) >= self.num_init and not is_sobol_generation_strategy:
                # need to deepcopy ax_client, so it doesn't change underlying parameter bounds
                acqf_value = self.get_acqf_value(copy.deepcopy(self.ax_client), copy.deepcopy(observation_feature))

            _, no_duplicate_options, max_cluster_cpu, max_cluster_memory = self.calculate_remaining_resources(
                opt_result.max_profiling_cpu, opt_result.max_profiling_memory,
                scaler=1.0,
                resource_options=opt_result.cand_profiling_options,
                write_log=write_log
            )
            # how many resources already, when looking at all suggestions so far?
            total_cpu: bool = sum([sug.configs.total_cpu for sug in suggestions])
            total_memory: bool = sum([sug.configs.total_memory for sug in suggestions])
            # get config associated with encoding (features)
            f_list = self.embedder.reconstruct(OrderedDict(sorted(params.items())))
            conf: Optional[ConfigurationModel] = next((w for w in no_duplicate_options if self.embedder.vectorize(w) == f_list), None)
            if conf is not None:
                total_cpu += conf.total_cpu
                total_memory += conf.total_memory
                write_log and logging.info(f"acqf_value={acqf_value}, "
                                           f"total_memory={total_memory}, max_cluster_memory={max_cluster_memory}")

            # add if valid config and sufficient resources
            has_res, res_ratio = self.has_resources(total_cpu, max_cluster_cpu, total_memory, max_cluster_memory)
            valid_cand: bool = (conf is not None and
                                all([conf.config_name != cm.config_name for cm in bottlenecked_configs_at_lower_rates]) and
                                all([conf.config_name != cm.config_name for cm in stable_configs_at_higher_rates]))
            if valid_cand and has_res:
                write_log and logging.info(f"Add trial No. {trial_idx} to candidate-list. "
                                           f"Remaining time: {get_remaining_time()}")
                suggestions.append(SuggestionModel(acqf_value=acqf_value, configs=conf))
                # assert no duplicates
                num_configurations: int = len(set([sugg.configs.config_name for sugg in suggestions]))
                num_suggestions: int = len(suggestions)
                assert num_configurations == num_suggestions, "Duplicate configurations have been suggested!"
                write_log and logging.info(f"#candidates: {len(suggestions)}")
            else:
                # In this case, "conf" is either invalid or not of interest (in terms of resources)
                # consequence: we do not abandon the trial, but return resource-scaled values (if not RandomModelBridge)
                # This will make BoTorch look only in areas which have sufficient resources
                if not is_sobol_generation_strategy:
                    res_ratio = res_ratio if valid_cand else 20
                    self.ax_client.complete_trial(trial_index=trial_idx,
                                                  raw_data=self.evaluator.out_of_boundaries(request, res_ratio))
                    logging.error(f"Trial No. {trial_idx} is infeasible, return resource-scaled "
                                  f"values (res_ratio={res_ratio}) for it! "
                                  f"Remaining time: {get_remaining_time()}")
                else:
                    self.ax_client.abandon_trial(trial_idx, reason="Infeasible configuration.")
                    write_log and logging.error(f"Trial No. {trial_idx} is infeasible, abandon it! "
                                                f"Remaining time: {get_remaining_time()}")

        logging.info(f"Returning {len(suggestions)} candidates for profiling.")
        return suggestions
