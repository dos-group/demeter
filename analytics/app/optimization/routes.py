import logging
import time
import warnings
from functools import wraps

from fastapi import APIRouter, status, HTTPException, Depends
from fastapi.responses import JSONResponse

from app.common.logging_utils import log_request
from app.latency.models import LatencyModelImpl, LatencyModelProvider
from app.optimization.handler import handle_profile_request, handle_rescale_request
from app.optimization.schemes import EvaluationRequest, ProfileResponse, RescaleResponse, \
    ResponseStatusEnum, \
    SuggestionModel
from app.workload.models import WorkloadModelProvider, WorkloadModelImpl

optimization_router = APIRouter(
    prefix="/optimization",
    tags=["optimization"]
)

optimization_metadata: dict = {
    "name": "optimization",
    "description": "Endpoints for handling rescaling / profiling."
}

def measure_time(func):
    @wraps(func)
    def function_wrapper(*args, **kwargs):
        start_time = time.time()
        response = func(*args, **kwargs)
        logging.info(f"Time required to process request: {(time.time() - start_time):.3f}s")
        return response

    return function_wrapper


@optimization_router.post("/rescale",
                          name="Evaluate if we need a rescaling.",
                          response_model=RescaleResponse,
                          status_code=status.HTTP_200_OK)
@measure_time
def rescale(request: EvaluationRequest,
                  workload_model: WorkloadModelImpl = Depends(WorkloadModelProvider.get_instance("workload_rescale")),
                  latency_model: LatencyModelImpl = Depends(LatencyModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not workload_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{workload_model.__class__.__name__} has not been fitted!")


    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        content: dict
        try:
            content = handle_rescale_request(request, workload_model, latency_model)
        except BaseException as err:
            logging.error(err, exc_info=True)
            content = {"status": ResponseStatusEnum.ERROR,
                       "production_config": SuggestionModel(configs=request.current_profile.configs).dict()}
        return JSONResponse(content=content)


@optimization_router.post("/profile",
                          name="Suggest profiling runs.",
                          response_model=ProfileResponse,
                          status_code=status.HTTP_200_OK)
@measure_time
def profile(request: EvaluationRequest,
                  workload_model: WorkloadModelImpl = Depends(WorkloadModelProvider.get_instance("workload_profile")),
                  latency_model: LatencyModelImpl = Depends(LatencyModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not workload_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{workload_model.__class__.__name__} has not been fitted!")


    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", category=UserWarning)
        content: dict
        try:
            content = handle_profile_request(request, workload_model, latency_model)
        except BaseException as err:
            logging.error(err, exc_info=True)
            content = {"status": ResponseStatusEnum.ERROR,
                       "profiling_configs": []}
        return JSONResponse(content=content)
