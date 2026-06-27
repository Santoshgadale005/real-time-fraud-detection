from fastapi import APIRouter
from app.schemas.transaction import Transaction
from app.services.predictor import predict_transaction

router = APIRouter()


@router.post("/predict")
def predict(transaction: Transaction):
    result = predict_transaction(transaction.dict())
    return result