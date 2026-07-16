import os
import csv
from datetime import datetime

import joblib
import pandas as pd

from config.logger import logger

model = joblib.load("models/fraud_model.pkl")
encoder = joblib.load("models/label_encoder.pkl")