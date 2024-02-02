import copy
from typing import Union, Type, List
from fastapi import HTTPException, status
from pydantic import BaseModel


class JobModel(BaseModel):
    job: str = "TEST"


class SystemConfigurationModel(BaseModel):
    scale_out: int
    is_best: bool
    is_valid: bool


class ScheduledTaskResponse(BaseModel):
    message: str
    task_hash: str

    class Config:
        schema_extra = {
            "example": {
                "message": "This is an example message.",
                "task_hash": "2374ß9285720"
            }
        }


class ResponseFinalizer:
    def __init__(self):
        raise NotImplementedError

    @staticmethod
    def run(tuple_list: List[tuple], current_scale_out: int, pydantic_model_class: Type[SystemConfigurationModel],
            restriction: Union[int, float], previously_valid_scale_outs: List[int]):
        # tuple scheme: (scale_out, validation-value, ...)

        try:
            assert issubclass(pydantic_model_class, SystemConfigurationModel)

            # sort by scale-out
            tuple_list = sorted(tuple_list, key=lambda tup: tup[0])
            # get index of currently used scale-out
            index: int = next((idx for idx, tup in enumerate(tuple_list) if tup[0] == current_scale_out))

            def is_valid(idx: int):
                return (tuple_list[idx][1] < restriction) and (tuple_list[idx][0] in previously_valid_scale_outs)

            # create list of objects
            candidates: List[SystemConfigurationModel] = [pydantic_model_class(scale_out=tup[0],
                                                          recovery_time=tup[-1],
                                                          latency=tup[-1],
                                                          is_valid=is_valid(idx),
                                                          is_best=False) for idx, tup in enumerate(tuple_list)]

            # get index of first valid one in list
            is_best_idx: int = next((idx for idx, c in enumerate(candidates) if c.is_valid), -1)
            if is_best_idx >= 0:
                candidates[is_best_idx].is_best = True

            assert 0 <= sum((int(c.is_valid) for c in candidates)) <= len(candidates)
            assert 0 <= sum((int(c.is_best) for c in candidates)) <= 1

            # extract the respective element from list of candidates
            current = copy.deepcopy(candidates[index])

            return current, candidates

        except AssertionError as e:
            raise HTTPException(status_code=status.HTTP_406_NOT_ACCEPTABLE,
                                detail=f"ResponseFinalizer encountered some issues: '{e}'")
