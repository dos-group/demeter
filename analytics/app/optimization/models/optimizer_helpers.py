from typing import List

from ax import Models
from ax.modelbridge.generation_node import GenerationStep
from ax.modelbridge.generation_strategy import GenerationStrategy
from ax.service.ax_client import AxClient

from app.optimization.schemes import ProfileModel, EvaluationRequest


def create_sobol_generation_step(profiles: List[ProfileModel], min_trials_observed: int, seed: int):
    sobol_cond: bool = 0 <= len(profiles) < min_trials_observed
    if sobol_cond:
        return GenerationStep(
            model=Models.SOBOL,
            model_gen_kwargs={"optimizer_kwargs": {"joint_optimize": True}},
            model_kwargs={"seed": seed},
            num_trials=min_trials_observed,
            min_trials_observed=min_trials_observed,
            enforce_num_trials=False,
            should_deduplicate=True
        )
    return None


def manually_attach_trials(ax_client: AxClient,
                           profiles: List[ProfileModel], embedder, evaluator):
    for profile in profiles:
        parametrization = embedder.construct(embedder.vectorize(profile.configs))
        _, trial_index = ax_client.attach_trial(parametrization)
        ax_client.complete_trial(trial_index=trial_index, raw_data=evaluator(parametrization))
    return ax_client


def update_and_refit_model(ax_client: AxClient):
    # ensure ax_client.generation_strategy.model gets assigned
    # this triggers a model-retraining, although we do not really need the trial suggestion
    ax_client.generation_strategy.experiment = ax_client.experiment
    ax_client.generation_strategy._maybe_move_to_next_step()
    ax_client.generation_strategy._fit_or_update_current_model(None)
    return ax_client


def setup_moo_experiment(name: str, gs: GenerationStrategy, embedder,
                         request: EvaluationRequest):

    ax_client = AxClient(generation_strategy=gs,
                         verbose_logging=False,
                         enforce_sequential_optimization=False)

    ax_client.create_experiment(
        name=name,
        parameters=embedder.get_search_space_as_list(),
        # must obey to constraints
        outcome_constraints=list(request.constraints.get_constraint_definitions()),
        objectives=request.get_objective_definitions(),
        immutable_search_space_and_opt_config=True
    )
    return ax_client
