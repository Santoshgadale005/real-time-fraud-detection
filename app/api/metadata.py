import json
from pathlib import Path
from fastapi import APIRouter

router = APIRouter()

METADATA_FILE = Path("deployment/model_metadata.json")

@router.get("/metadata")
def get_model_metadata():
    with open(METADATA_FILE, "r") as file:
        metadata = json.load(file)
    return metadata