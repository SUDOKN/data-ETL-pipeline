import os
from dotenv import load_dotenv
from fastapi import FastAPI

# Load environment variables from .env at startup
load_dotenv(dotenv_path=os.path.join(os.path.dirname(__file__), '.env'))

# DEBUG: Check if environment variable loaded
print("AWS_REGION:", os.getenv("AWS_REGION"))

from api.routes.upload import router as upload_router
from api.routes.status import router as status_router
from api.routes.ontology import router as ontology_router


app = FastAPI()
app.include_router(upload_router)
app.include_router(status_router)
app.include_router(ontology_router)