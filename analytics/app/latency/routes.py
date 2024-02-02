from fastapi import APIRouter, status, BackgroundTasks, Depends, HTTPException

from app.common.logging_utils import log_request
from app.common.models import BasePredictionModel
from app.common.schemes import ScheduledTaskResponse
from app.latency.models import LatencyModelProvider, LatencyModelImpl
from app.latency.schemes import LatencyModelTrainingRequest, LatencyModelPredictionRequest, \
    LatencyModelPredictionResponse, LatencyModelEvaluationRequest

latency_router = APIRouter(
    prefix="/latency",
    tags=["latency"]
)

latency_metadata: dict = {
    "name": "latency",
    "description": "Endpoints for interacting with latency model."
}


@latency_router.post("/training",
                     response_model=ScheduledTaskResponse,
                     name="Train the Latency model.",
                     status_code=status.HTTP_200_OK)
async def train_latency_model(request: LatencyModelTrainingRequest,
                              background_tasks: BackgroundTasks,
                              model: BasePredictionModel = Depends(LatencyModelProvider.get_instance())):
    log_request(request)
    background_tasks.add_task(model.fit, request)
    return ScheduledTaskResponse(message="Added latency-model to the list of background tasks.",
                                 task_hash=hash(background_tasks.tasks[-1]))


@latency_router.post("/prediction",
                     response_model=LatencyModelPredictionResponse,
                     name="Get latency-information about current and alternative scale-outs.",
                     status_code=status.HTTP_200_OK)
async def predict_with_latency_model(request: LatencyModelPredictionRequest,
                                     model: BasePredictionModel = Depends(LatencyModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{model.__class__.__name__} has not been fitted!")

    current, candidates = model.predict(request)
    return LatencyModelPredictionResponse(current=current, candidates=candidates)


@latency_router.post("/evaluation",
                     response_model=LatencyModelPredictionResponse,
                     name="Get latency-information about current and alternative scale-outs.",
                     status_code=status.HTTP_200_OK)
async def evaluate_with_latency_model(request: LatencyModelEvaluationRequest,
                                      model: LatencyModelImpl = Depends(LatencyModelProvider.get_instance())):
    log_request(request)
    # sanity check
    if not model.is_fitted:
        raise HTTPException(status_code=status.HTTP_412_PRECONDITION_FAILED,
                            detail=f"{model.__class__.__name__} has not been fitted!")

    current, candidates = model.evaluate(request)
    return LatencyModelPredictionResponse(current=current, candidates=candidates)
