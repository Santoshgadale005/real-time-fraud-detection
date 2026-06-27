from fastapi import FastAPI
from app.api.predict import router as predict_router

app = FastAPI(
    title="Real-Time Fraud Detection API",
    description="API for detecting fraudulent financial transactions",
    version="1.0.0"
)

# Home endpoint
@app.get("/")
def home():
    return {
        "message": "Welcome to the Real-Time Fraud Detection API"
    }

# Include prediction routes
app.include_router(predict_router)