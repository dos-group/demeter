from typing import Dict
from collections import OrderedDict
from fastapi import APIRouter, status, BackgroundTasks
from fastapi.responses import JSONResponse
from sklearn.linear_model import LinearRegression
import numpy as np
from app.workload.schemes import TimeSeries

common_router = APIRouter(
    prefix="/common",
    tags=["common"]
)

common_metadata: dict = {
    "name": "common",
    "description": "Shared endpoints."
}


@common_router.get("/tasks/{task_hash}",
                   name="Check the hash code of a submitted task, in order to verify if its still running.",
                   status_code=status.HTTP_200_OK)
async def check_hash_code(task_hash: str, background_tasks: BackgroundTasks):
    return any([task_hash == hash(task) for task in background_tasks.tasks])


@common_router.post("/regression",
                    name="Return the slope for any given workload.",
                    response_model=Dict[str, Dict[str, float]],
                    status_code=status.HTTP_200_OK)
async def calculate_slopes(request: Dict[str, TimeSeries]):
    result_dict: dict = OrderedDict()

    for identifier, timeseries in request.items():
        result_dict[identifier] = OrderedDict()

        timestamps, values = TimeSeries.unfold(timeseries)

        # fit the regressor
        regressor: LinearRegression = LinearRegression()
        regressor.fit(np.array(timestamps).reshape(-1, 1), np.array(values).reshape(-1, 1))

        # our data is one-dimensional, so we only have 1 coefficient / gradient
        result_dict[identifier]["slope"] = regressor.coef_.reshape(-1)[0]
        result_dict[identifier]["intercept"] = regressor.intercept_.reshape(-1)[0]

    return JSONResponse(status_code=status.HTTP_200_OK, content=result_dict)

