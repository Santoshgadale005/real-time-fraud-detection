from fastapi import APIRouter
from time import time

from app.schemas.transaction import Transaction
from models.predict import predict_transaction

from app.metrics import (
    prediction_requests,
    fraud_predictions,
    normal_predictions,
    prediction_latency,
)

router = APIRouter()


@router.post("/predict")
def predict(transaction: Transaction):
    start = time()

    # Count total prediction requests
    prediction_requests.inc()

    # Perform prediction
    result = predict_transaction(transaction.dict())

    # Count fraud/normal predictions
    prediction = result.get("prediction", 0)

    if prediction == 1:
        fraud_predictions.inc()
    else:
        normal_predictions.inc()

    # Record latency
    prediction_latency.observe(time() - start)

    return result