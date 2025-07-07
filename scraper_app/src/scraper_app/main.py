from fastapi import FastAPI
from scraper_app.api.routes.routes import router as routes_router

app = FastAPI()
app.include_router(routes_router)


@app.get("/")
def read_root():
    return {"message": "Hello from scraper_app!"}
