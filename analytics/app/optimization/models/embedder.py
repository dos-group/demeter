import copy
import logging
from collections import OrderedDict
from typing import List, Union, Dict, Tuple

import numpy as np
from ax import SearchSpace, Parameter, FixedParameter
from ax.core.parameter import PARAMETER_PYTHON_TYPE_MAP, ChoiceParameter, _get_parameter_type

from app.optimization.schemes import SearchSpaceModel, ConfigurationModel, ProfileModel


class EmbedderBO:
    def __init__(self, search_space: SearchSpaceModel, default_configs: ConfigurationModel):
        self.__search_space__: SearchSpaceModel = search_space
        self.__default_configs__: ConfigurationModel = default_configs
        self.parameters: List[Union[ChoiceParameter, FixedParameter]] = self.get_parameters()

    @property
    def options(self) -> List[ConfigurationModel]:
        all_pairs = np.array(np.meshgrid(
            *[param.values if isinstance(param, ChoiceParameter) else [param.value] for param in self.parameters]
        )).T.reshape(-1, len(self.parameters))
        conf_options: List[ConfigurationModel] = [ConfigurationModel(**self.construct(pair)) for pair in all_pairs]
        # only consider configs with 'full' task managers
        logging.debug("Configuration Filtering according to 'full' task-managers:")
        logging.debug(f"---> Before: {len(conf_options)}")
        conf_options = [cf for cf in conf_options if cf.scaleout % cf.task_slots == 0]
        logging.debug(f"---> After: {len(conf_options)}")
        # only consider configs with less or equal resources then default configs
        logging.debug("Configuration Filtering according to resources of default configs:")
        logging.debug(f"---> Before: {len(conf_options)}")
        conf_options = [cf for cf in conf_options
                        if cf.total_cpu <= self.__default_configs__.total_cpu
                        and cf.total_memory <= self.__default_configs__.total_memory]
        logging.debug(f"---> After: {len(conf_options)}")
        return conf_options


    def get_parameters(self):

        range_dict: Dict[str, Tuple[Tuple[float, float], float, bool]] = {}
        for key in set(["_".join(k.split("_")[1:]) for k in self.__search_space__.dict().keys()]):
            range_dict[key] = self.__search_space__.construct_boundaries_and_step_size(key)
        range_dict.pop("throughput_rate", None)

        parameters: List[Union[ChoiceParameter, FixedParameter]] = []
        for key, (boundaries, step_size, flip) in OrderedDict(sorted(range_dict.items())).items():
            min_val, max_val = list(sorted(boundaries))
            candidate_values = np.arange(min_val, max_val+step_size, step=step_size).astype(float)
            if flip:
                candidate_values = np.flip(candidate_values)
            parameter: Union[ChoiceParameter, FixedParameter]
            if len(candidate_values) > 1:
                parameter = ChoiceParameter(name=key,
                                            parameter_type=_get_parameter_type(type(min_val)),
                                            values=candidate_values.tolist(),
                                            sort_values=False,
                                            is_ordered=True)
            else:
                parameter = FixedParameter(name=key,
                                           parameter_type=_get_parameter_type(type(min_val)),
                                           value=candidate_values[0])
            parameters.append(parameter)
        return parameters

    def vectorize(self, obj: Union[ConfigurationModel, ProfileModel]) -> List[float]:
        features: List[float] = []
        config: ConfigurationModel = ConfigurationModel(**obj.dict())

        for key, value in sorted(config.dict().items()):
            param: Union[ChoiceParameter, FixedParameter] = next((param for param in self.parameters if param.name == key))
            rep_value: float
            if isinstance(param, ChoiceParameter):
                rep_value = next((v for v in sorted(param.values) if value == v))
            elif isinstance(param, FixedParameter):
                rep_value = param.value
            else:
                raise ValueError(f"Parameter is not of type 'ChoiceParameter' or 'FixedParameter'!")
            features.append(rep_value)
        return features

    def reconstruct(self, source_dict: dict):
        return list(OrderedDict(sorted(source_dict.items())).values())

    def construct(self, source_list: list):
        new_dict: dict = {param.name: value for param, value in zip(self.parameters, source_list)}
        return OrderedDict(sorted(new_dict.items()))

    def get_search_space(self):
        return SearchSpace(parameters=self.parameters)

    def get_search_space_as_list(self):
        map_dict = {"ChoiceParameter": "choice", "FixedParameter": "fixed"}

        def _parameter_to_dict(param: Parameter):
            value_dict = copy.deepcopy(param.__dict__)
            value_dict.pop("_sort_values", None)

            value_dict["_value_type"] = PARAMETER_PYTHON_TYPE_MAP[value_dict.pop("_parameter_type")].__name__
            value_dict["_type"] = map_dict.get(param.__class__.__name__)
            return {k[1:]: v for k, v in value_dict.items()}

        return [_parameter_to_dict(p) for p in self.parameters]
