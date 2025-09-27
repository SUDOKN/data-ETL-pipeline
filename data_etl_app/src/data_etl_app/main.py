import os
import logging
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

logging.basicConfig(
    level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
from core.dependencies.load_core_env import load_core_env
from open_ai_key_app.dependencies.load_open_ai_app_env import load_open_ai_app_env
from data_etl_app.dependencies.load_data_etl_env import load_data_etl_env

# Load environment variables
load_core_env()
load_data_etl_env()
load_open_ai_app_env()

log_level = os.getenv("LOG_LEVEL", "INFO").upper()
logger = logging.getLogger(__name__)

from core.utils.mongo_client import init_db
from core.dependencies.aws_clients import (
    initialize_core_aws_clients,
    cleanup_core_aws_clients,
)
from data_etl_app.dependencies.aws_clients import (
    initialize_data_etl_aws_clients,
    cleanup_data_etl_aws_clients,
)


@asynccontextmanager
async def lifespan(app: FastAPI):

    # Startup

    await init_db()
    logger.info("Database initialized successfully")

    await initialize_data_etl_aws_clients()
    await initialize_core_aws_clients()
    logger.info("Application startup complete")
    yield

    # Shutdown
    await cleanup_data_etl_aws_clients()
    await cleanup_core_aws_clients()
    logger.info("Application shutting down")


app = FastAPI(lifespan=lifespan)


@app.exception_handler(ValueError)
async def value_error_handler(request, exc: ValueError):
    return JSONResponse(
        status_code=400, content={"error": "Validation Error", "detail": str(exc)}
    )


@app.exception_handler(RequestValidationError)
async def validation_exception_handler(request: Request, exc: RequestValidationError):
    """
    Custom handler for request validation errors that includes field descriptions
    """
    errors = []
    for error in exc.errors():
        error_detail = {
            "type": error["type"],
            "loc": error["loc"],
            "msg": error["msg"],
            "input": error.get("input"),
        }

        # Add field description for query parameters if available
        if len(error["loc"]) >= 2 and error["loc"][0] == "query":
            field_name = error["loc"][1]

            # Get the route function and its parameters
            route = request.scope.get("route")
            if route and hasattr(route, "endpoint"):
                import inspect

                sig = inspect.signature(route.endpoint)
                param = sig.parameters.get(field_name)

                if param and hasattr(param.default, "description"):
                    error_detail["description"] = param.default.description

        errors.append(error_detail)

    return JSONResponse(
        status_code=422, content={"detail": errors, "error": "Request Validation Error"}
    )


# add a /health endpoint
@app.get("/health")
async def health_check():
    """
    Health check endpoint to verify if the application is running.
    """
    return {"status": "ok", "message": "Application is running"}


from data_etl_app.api.routes.knowledge.ontology import router as ontology_router
from data_etl_app.api.routes.knowledge.prompt import router as prompt_router
from data_etl_app.api.routes.manufacturer_user_form import (
    router as manufacturer_user_form_router,
)
from data_etl_app.api.routes.ground_truth.binary_ground_truth import (
    router as binary_ground_truth_router,
)
from data_etl_app.api.routes.ground_truth.concept_ground_truth import (
    router as concept_ground_truth_router,
)
from data_etl_app.api.routes.ground_truth.keyword_ground_truth import (
    router as keyword_ground_truth_router,
)

app.include_router(ontology_router)
app.include_router(prompt_router)
app.include_router(manufacturer_user_form_router)
app.include_router(binary_ground_truth_router)
app.include_router(concept_ground_truth_router)
app.include_router(keyword_ground_truth_router)

"""
USAGE: 
- From the data_etl_app directory: `data-ETL-pipeline/data_etl_app`
- run `PYTHONPATH=src uvicorn data_etl_app.main:app --reload`
"""
