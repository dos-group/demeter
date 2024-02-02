from fastapi import APIRouter, status, Depends, HTTPException

from app.common.logging_utils import log_request
from app.workload.models import WorkloadModelProvider, WorkloadModelImpl
from app.workload.schemes import WorkloadModelPredictionRequest, WorkloadModelPredictionResponse, TimeSeries

workload_router = APIRouter(
    prefix="/workload",
    tags=["workload"]
)

workload_metadata: dict = {
    "name": "workload",
    "description": "Endpoints for interacting with workload forecasting model."
}


@workload_router.post("/prediction",
                      response_model=WorkloadModelPredictionResponse,
                      name="Predict future throughput rates.",
                      status_code=status.HTTP_200_OK)
async def predict_throughput(request: WorkloadModelPredictionRequest,
                             model: WorkloadModelImpl = Depends(WorkloadModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{model.__class__.__name__} has not been fitted!")

    max_forecasting_period: int = int(model.num_models * model.step_size)
    if request.prediction_period_in_s > max_forecasting_period:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{request.prediction_period_in_s}s exceeds "
                                   f"maximum forecasting period ({max_forecasting_period}s)")

    workload: TimeSeries = model.predict(request)
    return WorkloadModelPredictionResponse(workload=workload)
