import os
from dotenv import load_dotenv
from fastapi import FastAPI

# Load environment variables from .env at startup
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), ".env"))

from data_etl_app.api.routes.ontology import router as ontology_router

app = FastAPI()
app.include_router(ontology_router)
