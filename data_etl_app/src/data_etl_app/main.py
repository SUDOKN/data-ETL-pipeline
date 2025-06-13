import os
from dotenv import load_dotenv
from fastapi import FastAPI

# Load environment variables from .env at startup
load_dotenv(
    dotenv_path=os.path.join(os.path.dirname(os.path.dirname(__file__)), ".env")
)

from api.routes.ontology import router as ontology_router

app = FastAPI()
app.include_router(ontology_router)

"""
USAGE: 
`uvicorn main:app --reload --app-dir src` or 
`uvicorn src.main:app --reload` if running from the project data_etl_app directory
"""
