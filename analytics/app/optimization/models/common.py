import dataclasses
from typing import List, Type

from ax.core.search_space import SearchSpaceDigest
from ax.models.torch.botorch_modular.default_options import mk_ehvi_default_optimizer_options, \
    register_default_optimizer_options
from ax.models.torch.botorch_modular.surrogate import Surrogate
from botorch import fit_gpytorch_model
from botorch.acquisition.input_constructors import acqf_input_constructor, construct_inputs_qNEHVI
from botorch.acquisition.multi_objective import qNoisyExpectedHypervolumeImprovement
from botorch.models import ModelListGP
from botorch.utils.containers import TrainingData
from gpytorch.mlls import SumMarginalLogLikelihood

from app.optimization.models.bo import BOModel


class CustomModelListGP(ModelListGP):
    def __init__(self, *models: BOModel):
        super().__init__(*models)
        self.custom_models: List[BOModel] = [*models]


class CustomSurrogate(Surrogate):
    def fit(
            self,
            training_data: TrainingData,
            search_space_digest: SearchSpaceDigest,
            metric_names: List[str],
            **kwargs
    ) -> None:
        """We override this function because we don't fit the model the 'classical way'."""
        self.construct(
            training_data=training_data,
            metric_names=metric_names,
            **dataclasses.asdict(search_space_digest),
            **kwargs
        )


class CustomqNoisyExpectedHypervolumeImprovement(qNoisyExpectedHypervolumeImprovement):
    pass


@acqf_input_constructor(CustomqNoisyExpectedHypervolumeImprovement)
def construct_custom_inputs_qNEHVI(*args, **kwargs):
    inputs = construct_inputs_qNEHVI(*args, **kwargs)
    inputs["cache_root"] = kwargs.get("cache_root", True)
    return inputs

register_default_optimizer_options(
    acqf_class=CustomqNoisyExpectedHypervolumeImprovement,
    default_options=mk_ehvi_default_optimizer_options(),
)

def get_fitted_model(train_X: list, train_Y: list, train_Yvar: list,
                     opt_class: Type[BOModel],
                     state_dict=None, refit: bool = True, normalize: bool = True, **kwargs):
    models = []
    for idx, (train_x, train_y, train_yvar) in enumerate(zip(train_X, train_Y, train_Yvar)):
        model = opt_class(train_x, train_y, train_yvar, normalize=normalize)
        models.append(model)

    model = CustomModelListGP(*models)
    if state_dict is not None:
        model.load_state_dict(state_dict)
    if state_dict is None or refit:
        mll = SumMarginalLogLikelihood(model.likelihood, model).to(train_X[0])
        fit_gpytorch_model(mll)
    return model
