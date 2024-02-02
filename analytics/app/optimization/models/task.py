import copy
import logging
from typing import List

from ax.modelbridge.generation_node import GenerationStep
from ax.modelbridge.generation_strategy import GenerationStrategy
from ax.modelbridge.registry import Cont_X_trans, Models, Y_trans
from botorch.utils.containers import TrainingData
from gpytorch.mlls import SumMarginalLogLikelihood

from app.optimization.models.bo import BOModel
from app.optimization.models.common import CustomModelListGP, get_fitted_model, CustomSurrogate, \
    CustomqNoisyExpectedHypervolumeImprovement
from app.optimization.models.embedder import EmbedderBO
from app.optimization.models.evaluator import EvaluatorBO
from app.optimization.models.optimizer import OptimizerBO
from app.optimization.models.optimizer_helpers import create_sobol_generation_step, setup_moo_experiment, manually_attach_trials, \
    update_and_refit_model
from app.optimization.schemes import ProfileModel, EvaluationRequest


class TASKModel(CustomModelListGP):
    _num_outputs = 4  # metadata for botorch

    @classmethod
    def construct_inputs(cls, training_data: TrainingData, **kwargs):
        initial_profiles: List[ProfileModel] = kwargs["profiled_workloads"]
        min_thr_rate = round(min([p.metrics.throughput_rate for p in initial_profiles], default=None), 2)
        max_thr_rate = round(max([p.metrics.throughput_rate for p in initial_profiles], default=None), 2)
        thr_rate_string = f"min_thr_rate={min_thr_rate}, max_thr_rate={max_thr_rate}"

        model = get_fitted_model(training_data.Xs, training_data.Ys, training_data.Yvars,
                                 BOModel,
                                 state_dict=kwargs.get("state_dict", None),
                                 refit=kwargs.get("refit", True),
                                 normalize=kwargs.get("normalize", True))

        logging.debug(f"TASK model obtained with {len(initial_profiles)} samples ({thr_rate_string})...")
        return {"model": model}

    def __init__(self, model: CustomModelListGP):
        super().__init__(*model.models)


class OptimizerTASK(OptimizerBO):

    def __init__(self, request: EvaluationRequest, **kwargs):
        super().__init__(**kwargs)

        initial_profiles: List[ProfileModel] = kwargs.get("initial_profiles", [])

        # vectorizer and evaluator
        self.embedder: EmbedderBO = EmbedderBO(request.search_space, request.default_configs)
        self.evaluator: EvaluatorBO = EvaluatorBO(initial_profiles, self.embedder)

        transforms = Cont_X_trans if self.normalize else Cont_X_trans + Y_trans
        # setup experiment, get initial starting points
        gen_stra: GenerationStrategy = GenerationStrategy(
            steps=[gs for gs in [
                # quasi-random generation of initial points
                create_sobol_generation_step(initial_profiles, self.num_init, self.seed),
                GenerationStep(
                    model=Models.MOO_MODULAR,
                    num_trials=-1,
                    enforce_num_trials=True,
                    should_deduplicate=True,
                    model_gen_kwargs=self.exp_config.get("model_gen_kwargs", None),
                    model_kwargs={
                        **self.exp_config.get("model_kwargs", {"fit_out_of_design": True}),
                        "transforms": transforms,
                        # Wrap MOOModel with CustomSurrogate
                        "surrogate": CustomSurrogate(botorch_model_class=TASKModel,
                                                     model_options={
                                                         "normalize": self.normalize,
                                                         # this is an important ref-link! DO NOT REMOVE
                                                         "profiled_workloads": initial_profiles
                                                     },
                                                     mll_class=SumMarginalLogLikelihood),
                        # MC-based batch Noisy Expected (HyperVolume) Improvement
                        "botorch_acqf_class": CustomqNoisyExpectedHypervolumeImprovement,
                        "acquisition_options": self.exp_config.get("acquisition_options", None)
                    }
                ),
            ] if gs is not None]
        )

        exp_name: str = "demeter_experiment_task"
        self.ax_client = setup_moo_experiment(exp_name, gen_stra, self.embedder, request)

        self.ax_client = manually_attach_trials(self.ax_client, initial_profiles, self.embedder, self.evaluator)
        try:
            # fit model and save it
            self.ax_client = update_and_refit_model(self.ax_client)
            if hasattr(self.ax_client.generation_strategy.model.model, "surrogate"):
                # 'SobolGenerator' object has no attribute 'surrogate'
                self.best_model = copy.deepcopy(self.ax_client.generation_strategy.model.model.surrogate.model)
        except BaseException as e:
            logging.error(e, exc_info=True)
            pass
