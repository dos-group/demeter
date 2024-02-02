import logging
import random
from typing import List, Optional

import torch
from ax import Models
from ax.modelbridge.generation_node import GenerationStep
from ax.modelbridge.generation_strategy import GenerationStrategy
from ax.modelbridge.registry import Cont_X_trans, Y_trans
from botorch.models.gpytorch import GPyTorchModel
from botorch.sampling import SobolQMCNormalSampler
from botorch.utils.containers import TrainingData
from gpytorch import lazify
from gpytorch.distributions import MultitaskMultivariateNormal
from gpytorch.lazy import PsdSumLazyTensor, BlockDiagLazyTensor
from gpytorch.likelihoods import LikelihoodList
from gpytorch.mlls import SumMarginalLogLikelihood
from gpytorch.models import GP

from app.optimization.models.bo import BOModel
from app.optimization.models.common import get_fitted_model, CustomModelListGP, CustomSurrogate, \
    CustomqNoisyExpectedHypervolumeImprovement
from app.optimization.models.embedder import EmbedderBO
from app.optimization.models.evaluator import EvaluatorBO
from app.optimization.models.optimizer import OptimizerBO
from app.optimization.models.optimizer_helpers import create_sobol_generation_step, setup_moo_experiment, manually_attach_trials, \
    update_and_refit_model
from app.optimization.schemes import ProfileModel, EvaluationRequest


class RGPEModel(GP, GPyTorchModel):
    _num_outputs = 4  # metadata for botorch

    @staticmethod
    def compute_ranking_loss(f_samps, target_y):
        def roll_col(X, shift):
            return torch.cat((X[..., -shift:], X[..., :-shift]), dim=-1)

        n = target_y.shape[0]
        if f_samps.ndim == 3:
            # Compute ranking loss for target model
            # take cartesian product of target_y
            cartesian_y = torch.cartesian_prod(
                target_y.squeeze(-1),
                target_y.squeeze(-1),
            ).view(n, n, 2)
            # the diagonal of f_samps are the out-of-sample predictions
            # for each LOO model, compare the out of sample predictions to each in-sample prediction
            rank_loss = (
                    (f_samps.diagonal(dim1=1, dim2=2).unsqueeze(-1) < f_samps) ^
                    (cartesian_y[..., 0] < cartesian_y[..., 1])
            ).sum(dim=-1).sum(dim=-1)
        else:
            rank_loss = torch.zeros(f_samps.shape[0], dtype=torch.long, device=target_y.device)
            y_stack = target_y.squeeze(-1).expand(f_samps.shape)
            for i in range(1, target_y.shape[0]):
                rank_loss += (
                        (roll_col(f_samps, i) < f_samps) ^ (roll_col(y_stack, i) < y_stack)
                ).sum(dim=-1)
        return rank_loss

    @staticmethod
    def get_target_model_loocv_sample_preds(training_data: TrainingData, target_model, num_samples):
        batch_size = len(training_data.Xs[0])
        masks = torch.eye(len(training_data.Xs[0]), dtype=torch.uint8, device=training_data.Xs[0].device).bool()

        train_X_cv = [torch.stack([train_x[~m] for m in masks]) for train_x in training_data.Xs]
        train_Y_cv = [torch.stack([train_y[~m] for m in masks]) for train_y in training_data.Ys]
        train_Yvar_cv = [torch.stack([train_yvar[~m] for m in masks]) for train_yvar in training_data.Yvars]

        state_dict = target_model.state_dict()
        # expand to batch size of batch_mode LOOCV model
        state_dict_expanded = {
            name: t.expand(batch_size, *[-1 for _ in range(t.ndim)])
            for name, t in state_dict.items()
        }
        model = get_fitted_model(train_X_cv, train_Y_cv, train_Yvar_cv,
                                 BOModel,
                                 state_dict=state_dict_expanded, refit=False, normalize=True)
        with torch.no_grad():
            posterior = model.posterior(training_data.Xs[0])
            # Since we have a batch mode gp and model.posterior always returns an output dimension,
            # the output from `posterior.sample()` here `num_samples x n x n x 1`, so let's squeeze
            # the last dimension.
            sampler = SobolQMCNormalSampler(num_samples=num_samples)
            return sampler(posterior).squeeze(-1)

    @staticmethod
    def compute_rank_weights(training_data: TrainingData, base_models, target_model, num_samples):
        ranking_losses = [[] for _ in range(len(training_data.Ys))]

        def compute_and_append(samples):
            slices = [torch.index_select(samples, -1, idx) for idx in
                      torch.tensor(list(range(len(training_data.Ys))), device=samples.device)]
            for idx, (samples_slice, train_y) in enumerate(zip(slices, training_data.Ys)):
                samples_slice = torch.squeeze(samples_slice)
                # compute and save ranking loss
                ranking_losses[idx].append(RGPEModel.compute_ranking_loss(samples_slice, train_y))

        def count_and_weight(tensor_list: List[torch.Tensor]):
            if not len(tensor_list):
                return torch.tensor([]).reshape(1, -1)

            t_tensor: torch.Tensor = torch.stack(tensor_list).to(torch.double)

            # perform model pruning to prevent weight dilution (RGPE v4)
            alpha: float = 0.0
            number_of_function_evaluations: float = 20.0
            t_tensor_max = torch.amax(t_tensor)
            for i in range(len(base_models)):
                better_than_target = torch.sum(t_tensor[i, :] < t_tensor[-1, :])
                worse_than_target = torch.sum(t_tensor[i, :] >= t_tensor[-1, :])
                correction_term = alpha * (better_than_target + worse_than_target)
                proba_keep = better_than_target / (better_than_target + worse_than_target + correction_term)
                proba_keep = proba_keep * (1 - training_data.Xs[0].shape[-2] / number_of_function_evaluations)
                r = random.random()
                if r > proba_keep:
                    t_tensor[i, :] = t_tensor_max * 2 + 1

            # compute best model (minimum ranking loss) for each sample
            best_models = torch.zeros_like(t_tensor)
            # in case of tie: distribute the weight fairly among all participants of the tie (RGPE v4)
            minima = torch.amin(t_tensor, dim=0)
            assert len(minima) == num_samples
            # Compute boolean mask of minimum values
            min_mask = t_tensor == minima
            row_indices, col_indices = min_mask.nonzero(as_tuple=True)
            # Count number of occurrences of minimum value for each column, compute weight for each minimum value
            best_models[row_indices, col_indices] = (1.0 / torch.sum(min_mask, dim=0, dtype=t_tensor.dtype))[col_indices]
            best_models = best_models.sum(dim=-1)
            # Compute proportion of samples for which each model is best
            proportions = best_models / t_tensor.shape[1]
            return proportions.reshape(1, -1)

        # compute ranking loss for each base model (RGPE v1)
        for task in range(len(base_models)):
            model = base_models[task]
            # compute posterior over training points for target task
            posterior = model.posterior(training_data.Xs[0])
            sampler = SobolQMCNormalSampler(num_samples=num_samples)
            base_f_samps = sampler(posterior).squeeze(-1)
            compute_and_append(base_f_samps)

        # compute ranking loss for target model using LOOCV (RGPE v1)
        target_f_samps = RGPEModel.get_target_model_loocv_sample_preds(training_data, target_model, num_samples)
        compute_and_append(target_f_samps)

        # numObjectives x numModels
        rank_weights = torch.cat([count_and_weight(rl) for rl in ranking_losses], dim=0).to(training_data.Xs[0])
        # numModels x numObjectives
        return rank_weights.transpose(0, 1)

    @classmethod
    def construct_inputs(cls, training_data: TrainingData, **kwargs):
        initial_profiles: List[ProfileModel] = kwargs["profiled_workloads"]
        min_thr_rate = round(min([p.metrics.throughput_rate for p in initial_profiles], default=None), 2)
        max_thr_rate = round(max([p.metrics.throughput_rate for p in initial_profiles], default=None), 2)
        thr_rate_string = f"min_thr_rate={min_thr_rate}, max_thr_rate={max_thr_rate}"

        num_posterior_samples = kwargs["num_posterior_samples"]
        prior_tasks = kwargs["prior_tasks"]

        task: Optional[CustomModelListGP] = None
        model_list: list = list(prior_tasks)
        rank_weights: torch.Tensor = torch.ones(len(model_list), len(training_data.Xs)) / len(model_list)
        if len(initial_profiles) > 1:
            task = get_fitted_model(training_data.Xs, training_data.Ys, training_data.Yvars,
                                    BOModel,
                                    state_dict=kwargs.get("state_dict", None),
                                    refit=kwargs.get("refit", True),
                                    normalize=kwargs.get("normalize", True))
            model_list += [task]
            rank_weights = torch.ones(len(model_list), len(training_data.Xs)) / len(model_list)
            if len(initial_profiles) >= 3:
                rank_weights: torch.Tensor = RGPEModel.compute_rank_weights(
                    training_data,
                    prior_tasks,
                    task,
                    num_posterior_samples,
                )

        if torch.numel(rank_weights) == 0:
            raise ValueError("Base-model is undefined + no support models found! "
                             "Most likely reason: Emulated data repository is incomplete!")

        logging.debug(f"RGPE model obtained with {len(initial_profiles)} samples ({thr_rate_string}), "
                      f"{len(prior_tasks)} prior tasks...")

        return {"models": model_list, "weights": rank_weights.to(training_data.Xs[0]), "target_task": task}

    def __init__(self, models: List[CustomModelListGP], weights: torch.Tensor,
                 target_task: Optional[CustomModelListGP]):
        super().__init__()
        self.models: List[CustomModelListGP] = models
        self.weights: torch.Tensor = weights
        self.target_task: Optional[CustomModelListGP] = target_task
        self.likelihood = LikelihoodList(*[m.likelihood for m in models])
        self.variance_mode: str = "average"
        self.to(weights)

    def forward(self, x):
        """New implementation, significantly more efficient."""
        weighted_means_list = []
        weighted_covars_list = []

        x = x.unsqueeze(-3) if x.ndim == 2 else x
        sample_count: int = x.size()[-2]

        # make local copy
        all_weights = self.weights.clone().detach()
        # filter model with zero weights
        # weights on covariance matrices are weight**2
        all_weights[all_weights ** 2 == 0] = 0
        # re-normalize
        all_weights = all_weights / all_weights.sum(dim=0, keepdim=True)

        for raw_idx, (model, weight_tensor) in enumerate(zip(self.models, all_weights)):
            posterior = model.posterior(x)
            posterior_mean = posterior.mean
            posterior_cov = posterior.mvn.covariance_matrix

            c_y_std_list, c_y_mean_list = zip(*[(m.custom_y_std, m.custom_y_mean) for m in model.custom_models])
            c_y_std, c_y_mean = torch.cat(c_y_std_list, -1), torch.cat(c_y_mean_list, -1)

            # small modifications to weight tensor and custom y-std so that transformation works
            m_weight_tensor = weight_tensor.repeat_interleave(sample_count)
            m_weight_tensor_self_mul = m_weight_tensor.unsqueeze(-1) * m_weight_tensor.unsqueeze(-2)
            m_c_y_std = torch.atleast_2d(c_y_std.repeat_interleave(sample_count))
            weighted_weights = m_weight_tensor_self_mul * torch.pow(m_c_y_std, 2)

            if self.target_task is None:
                weighted_mean = ((posterior_mean * c_y_std) + c_y_mean) * weight_tensor.reshape(c_y_mean.shape)
                weighted_cov = posterior_cov * weighted_weights.unsqueeze(-3)
            else:
                weighted_mean = posterior_mean * weight_tensor.reshape(c_y_mean.shape)
                weighted_cov = posterior_cov * m_weight_tensor_self_mul

            weighted_means_list.append(weighted_mean)
            if self.target_task is None or self.variance_mode == 'average':
                weighted_covars_list.append(lazify(weighted_cov.unsqueeze(-3)))
            elif self.variance_mode == 'target':
                if raw_idx + 1 == len(self.models):
                    weighted_covars_list.append(lazify(posterior_cov.unsqueeze(-3)))

        # set mean and covariance to be the rank-weighted sum the means and covariances of the
        # base models and target model
        if self.target_task is None:
            mean_x = torch.stack(weighted_means_list).sum(dim=0)
            covar_x = PsdSumLazyTensor(*weighted_covars_list)
        else:
            mean_x = torch.stack(weighted_means_list).sum(dim=0) * c_y_std + c_y_mean
            covar_x = PsdSumLazyTensor(*weighted_covars_list) * torch.pow(m_c_y_std, 2)

        covar_x = BlockDiagLazyTensor(covar_x, block_dim=-3)
        # try to squeeze first dimension, if possible
        mean_x = torch.atleast_2d(mean_x.squeeze(0))
        covar_x = covar_x.squeeze(0)
        return MultitaskMultivariateNormal(mean_x, covar_x, interleaved=False)


class OptimizerRGPE(OptimizerBO):

    def __init__(self, request: EvaluationRequest,
                 prior_tasks: List[CustomModelListGP], **kwargs):
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
                        # Wrap RGPEModel with CustomSurrogate
                        "surrogate": CustomSurrogate(botorch_model_class=RGPEModel,
                                                     model_options={
                                                         "num_posterior_samples": 1024,
                                                         # this is an important ref-link! DO NOT REMOVE
                                                         "profiled_workloads": initial_profiles,
                                                         "normalize": self.normalize,
                                                         "prior_tasks": prior_tasks
                                                     },
                                                     mll_class=SumMarginalLogLikelihood),
                        # MC-based batch Noisy Expected (HyperVolume) Improvement
                        "botorch_acqf_class": CustomqNoisyExpectedHypervolumeImprovement,
                        "acquisition_options": self.exp_config.get("acquisition_options", None)
                    }
                ),
            ] if gs is not None]
        )

        exp_name: str = "demeter_experiment_rgpe"
        self.ax_client = setup_moo_experiment(exp_name, gen_stra, self.embedder, request)

        self.ax_client = manually_attach_trials(self.ax_client, initial_profiles, self.embedder, self.evaluator)
        try:
            # fit model and save it
            self.ax_client = update_and_refit_model(self.ax_client)
        except BaseException as e:
            logging.error(e, exc_info=True)
            pass
