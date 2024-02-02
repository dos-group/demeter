import numpy as np
from typing import List, Tuple, Union, Dict

from app.optimization.models.embedder import EmbedderBO
from app.optimization.schemes import EvaluationRequest, ProfileModel


class EvaluatorBO:
    def __init__(self, profiles: List[ProfileModel], embedder: EmbedderBO):
        self.embedder: EmbedderBO = embedder
        self.profiles: List[ProfileModel] = profiles
        self.features: List[List[float]] = [self.embedder.vectorize(w.configs) for w in self.profiles]
        # assert unique encoding for each profile
        num_encodings: int = len([list(x) for x in set(tuple(x) for x in self.features)])
        num_profiles: int = len(self.profiles)
        assert num_encodings == num_profiles, "Profiles are not uniquely encoded!"

    @staticmethod
    def finalize_scores(scores: Dict[str, float], noise_sd: float = 0.1) -> Dict[str, Tuple[float, float]]:
        noise_arr: np.ndarray = np.random.normal(0, noise_sd, len(scores))
        return {k: (v + n, noise_sd) for (k, v), n in zip(scores.items(), noise_arr)}

    def out_of_boundaries(self, request: EvaluationRequest, resource_ratio: float) -> Dict[str, Tuple[float, float]]:
        """For 'invalid' configs, we need artificial values, to manipulate the BoTorch search in these areas."""
        factor: int = 0.1 * resource_ratio
        scores: Dict[str, float] = {
            **{k:(op.threshold * factor) for k, op in request.get_objective_definitions().items()},
            **{k:(v * factor) for k, v in request.constraints.get_constraint_definitions_as_dict().items()}
        }
        return self.finalize_scores(scores)

    def __call__(self, parameterization, *args, **kwargs) -> Dict[str, Tuple[float, float]]:
        feature_list: List[Union[int, float, str]] = self.embedder.reconstruct(parameterization)
        profile: ProfileModel = self.profiles[self.features.index(feature_list)]
        scores: Dict[str, float] = {**profile.get_objective_scores(), **profile.get_constraint_scores()}
        return self.finalize_scores(scores)





