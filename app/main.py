from fastapi import FastAPI

from app.api.predict import router as predict_router
from app.api.health import router as health_router
from app.api.metadata import router as metadata_router

from prometheus_fastapi_instrumentator import Instrumentator

app = FastAPI(
    title="Real-Time Fraud Detection API",
    version="1.0.0"
)

@app.get("/")
def home():
    return {
        "message": "Welcome to the Real-Time Fraud Detection API",
        "docs": "/docs",
        "health": "/health"
    }

app.include_router(predict_router)
app.include_router(health_router)
app.include_router(metadata_router)

# Prometheus Metrics
#Instrumentator().instrument(app).expose(app)