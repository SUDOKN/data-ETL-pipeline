import os
import logging
from contextlib import asynccontextmanager
from dotenv import load_dotenv
from fastapi import FastAPI, Request
from fastapi.responses import JSONResponse
from fastapi.exceptions import RequestValidationError

# Load environment variables from .env at startup
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
)

log_level = os.getenv("LOG_LEVEL", "INFO").upper()

logging.basicConfig(
    level=log_level, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

from shared.utils.mongo_client import init_db

from data_etl_app.api.routes.ontology import router as ontology_router
from data_etl_app.api.routes.ground_truth import router as ground_truth_router
from data_etl_app.dependencies.aws_deps import aws_clients


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    await init_db()
    logger.info("Database initialized successfully")

    await aws_clients.initialize()
    yield

    # Shutdown
    await aws_clients.cleanup()
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


app.include_router(ontology_router)
app.include_router(ground_truth_router)

"""
USAGE: 
- From the data_etl_app directory: `data-ETL-pipeline/data_etl_app`
- run `PYTHONPATH=src uvicorn data_etl_app.main:app --reload`
"""
