from typing import Optional

import torch
from botorch.models import FixedNoiseGP
from botorch.utils.containers import TrainingData
from torch import Tensor


class BOModel(FixedNoiseGP):
    _num_outputs = 1  # to inform GPyTorchModel API

    def __init__(self, train_x: Tensor, train_y: Tensor, train_yvar: Tensor, *args, **kwargs):
        self.custom_y_mean: Optional[torch.Tensor] = None
        self.custom_y_std: Optional[torch.Tensor] = None
        if kwargs.get("normalize", False):
            self.custom_y_mean = train_y.mean(dim=-2, keepdim=True)
            self.custom_y_std = train_y.std(dim=-2, keepdim=True)
            train_y = (train_y - self.custom_y_mean) / self.custom_y_std
        super().__init__(train_x, train_y, train_yvar.expand_as(train_y), **kwargs)

    @classmethod
    def construct_inputs(cls, training_data: TrainingData, **kwargs):
        return {
            "train_x": training_data.Xs[0],
            "train_y": training_data.Ys[0],
            "train_yvar": training_data.Yvars[0],
            **kwargs
        }
