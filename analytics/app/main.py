import sklearn
from fastapi import FastAPI, status
from fastapi.responses import JSONResponse, RedirectResponse

from app.common.logging_utils import init_logging


def start_server():
    from app.latency.routes import latency_router, latency_metadata
    from app.common.routes import common_router, common_metadata
    from app.recoverytime.routes import recoverytime_router, recoverytime_metadata
    from app.workload.routes import workload_router, workload_metadata
    from app.optimization.routes import optimization_router, optimization_metadata

    my_app = FastAPI(
        title="Demeter-Py",
        description="Lightweight python service for handling predictions in Demeter.",
        version="1.0.0",
        openapi_tags=[
            #common_metadata,
            #latency_metadata,
            #workload_metadata,
            #recoverytime_metadata,
            optimization_metadata
        ]
    )
    # routes
    #my_app.include_router(common_router)
    #my_app.include_router(latency_router)
    #my_app.include_router(recoverytime_router)
    #my_app.include_router(workload_router)
    my_app.include_router(optimization_router)

    @my_app.exception_handler(sklearn.exceptions.NotFittedError)
    async def not_fitted_error_handler(*args, **kwargs):
        return JSONResponse(
            status_code=status.HTTP_412_PRECONDITION_FAILED,
            content={"message": "The required model was not yet trained!"},
        )

    @my_app.get("/", include_in_schema=False)
    async def redirect():
        response = RedirectResponse(url='/docs')
        return response

    init_logging()
    # return app
    return my_app

app = start_server()
