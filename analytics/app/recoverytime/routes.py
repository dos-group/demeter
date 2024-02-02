from fastapi import APIRouter, status, Depends, BackgroundTasks, HTTPException

from app.common.logging_utils import log_request
from app.common.models import BasePredictionModel
from app.common.schemes import ScheduledTaskResponse
from app.recoverytime.models import RecoveryTimeModelImpl, RecoveryTimeModelProvider
from app.workload.models import WorkloadModelProvider, WorkloadModelImpl
from app.recoverytime.schemes import RecoveryTimeModelPredictionRequest, RecoveryTimeModelPredictionResponse, \
    RecoveryTimeModelTrainingRequest, RecoveryTimeModelEvaluationRequest

recoverytime_router = APIRouter(
    prefix="/recoverytime",
    tags=["recoverytime"]
)

recoverytime_metadata: dict = {
    "name": "recoverytime",
    "description": "Endpoints for getting recovery-time information."
}


@recoverytime_router.post("/training",
                          response_model=ScheduledTaskResponse,
                          name="Train the capacity model.",
                          status_code=status.HTTP_200_OK)
async def train_capacity_model(request: RecoveryTimeModelTrainingRequest,
                               background_tasks: BackgroundTasks,
                               model: BasePredictionModel = Depends(RecoveryTimeModelProvider.get_instance())):
    background_tasks.add_task(model.fit, request)
    return ScheduledTaskResponse(message="Added capacity-model to the list of background tasks.",
                                 task_hash=hash(background_tasks.tasks[-1]))


@recoverytime_router.post("/prediction",
                          response_model=RecoveryTimeModelPredictionResponse,
                          name="Predict recovery time.",
                          status_code=status.HTTP_200_OK)
async def predict_with_model(request: RecoveryTimeModelPredictionRequest,
                             rt_model: RecoveryTimeModelImpl = Depends(RecoveryTimeModelProvider.get_instance()),
                             wl_model: WorkloadModelImpl = Depends(WorkloadModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not rt_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{rt_model.__class__.__name__} has not been fitted!")
    if not wl_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{wl_model.__class__.__name__} has not been fitted!")

    max_forecasting_period: int = int(wl_model.num_models * wl_model.step_size)
    if request.prediction_period_in_s > max_forecasting_period:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{request.prediction_period_in_s}s exceeds "
                                   f"maximum forecasting period ({max_forecasting_period}s)")

    current, candidates, predicted_throughput_rate, slope = rt_model.predict(request, wl_model)
    return RecoveryTimeModelPredictionResponse(current=current,
                                               candidates=candidates,
                                               predicted_throughput_rate=predicted_throughput_rate, slope=slope)


@recoverytime_router.post("/evaluation",
                          response_model=RecoveryTimeModelPredictionResponse,
                          name="Predict recovery time.",
                          status_code=status.HTTP_200_OK)
async def evaluate_model(request: RecoveryTimeModelEvaluationRequest,
                         rt_model: RecoveryTimeModelImpl = Depends(RecoveryTimeModelProvider.get_instance()),
                         wl_model: WorkloadModelImpl = Depends(WorkloadModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not rt_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{rt_model.__class__.__name__} has not been fitted!")
    if not wl_model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{wl_model.__class__.__name__} has not been fitted!")

    max_forecasting_period: int = int(wl_model.num_models * wl_model.step_size)
    if request.prediction_period_in_s > max_forecasting_period:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{request.prediction_period_in_s}s exceeds "
                                   f"maximum forecasting period ({max_forecasting_period}s)")

    current, candidates, predicted_throughput_rate, slope = rt_model.evaluate(request, wl_model)
    return RecoveryTimeModelPredictionResponse(current=current,
                                               candidates=candidates,
                                               predicted_throughput_rate=predicted_throughput_rate, slope=slope)
