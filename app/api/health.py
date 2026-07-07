from fastapi import APIRouter

router = APIRouter()

@router.get("/health")
def health_check():
    return {
        "status": "healthy",
        "model_loaded": True,
        "version": "1.0.0"
    }