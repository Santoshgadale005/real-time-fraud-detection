# Day 6 Summary – Model Deployment with FastAPI

## Objective

The objective of Day 6 was to deploy the trained fraud detection model as a REST API using FastAPI. This allows external applications to send transaction data and receive fraud predictions in real time.

---

## Tasks Completed

### 1. Created FastAPI Project Structure

Organized the API into a modular architecture.

```
app/
├── api/
│   └── predict.py
├── schemas/
│   └── transaction.py
├── services/
│   └── predictor.py
└── main.py
```

---

### 2. Created Transaction Schema

- Used **Pydantic BaseModel**
- Defined the input fields required for prediction
- Enabled automatic request validation

Fields include:

- step
- type
- amount
- oldbalanceOrg
- newbalanceOrig
- oldbalanceDest
- newbalanceDest
- isFlaggedFraud

---

### 3. Implemented Prediction Service

Created `predictor.py` to:

- Load the trained Random Forest model
- Load the Label Encoder
- Convert incoming JSON into a DataFrame
- Encode categorical features
- Predict fraud status
- Return prediction confidence

---

### 4. Created Prediction API

Implemented the `/predict` endpoint using FastAPI.

Request Method:

```
POST /predict
```

The endpoint accepts transaction details and returns:

- Fraud prediction
- Prediction confidence

---

### 5. Built FastAPI Application

Configured:

- Application title
- API description
- Version
- Home endpoint (`/`)
- Prediction router

---

### 6. Tested the API

Started the server using:

```bash
uvicorn app.main:app --reload
```

Opened Swagger UI:

```
http://127.0.0.1:8000/docs
```

Successfully tested the API using sample transaction data.

Example Response:

```json
{
    "prediction": "Legitimate",
    "confidence": 1.0
}
```

---

## Technologies Used

- Python
- FastAPI
- Uvicorn
- Pydantic
- Pandas
- Scikit-learn
- Joblib

---

## Challenges Faced

- Created the FastAPI project structure.
- Corrected the location of the `app` folder.
- Loaded the trained model and label encoder correctly.
- Verified API functionality using Swagger UI.

---

## Learning Outcomes

- Learned how to deploy a machine learning model using FastAPI.
- Understood API routing and request validation.
- Learned to load trained models using Joblib.
- Built a REST API for fraud prediction.
- Tested APIs using Swagger documentation.

---

## Project Status

✅ Machine Learning model trained

✅ Model saved

✅ FastAPI application created

✅ Prediction API implemented

✅ Swagger UI tested successfully

---

## Next Day Plan (Day 7)

- Integrate the Kafka Consumer with the trained model.
- Perform real-time fraud prediction for streaming transactions.
- Store prediction results in MongoDB.
- Display live prediction results in the console.
- Prepare the system for dashboard integration.

---

## Conclusion

Day 6 successfully completed the deployment phase of the Real-Time Fraud Detection project. The trained machine learning model is now accessible through a FastAPI REST API, enabling real-time prediction requests and forming the foundation for streaming inference in the next phase of the project.